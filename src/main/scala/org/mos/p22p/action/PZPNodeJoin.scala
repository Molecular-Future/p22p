package org.mos.p22p.action

import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.p22p.exception.NodeInfoDuplicated;
import org.mos.p22p.exception.FBSException
import org.mos.p22p.PSMPZP
import org.mos.p22p.core.MessageSender
import org.mos.p22p.node.Networks
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PRetJoin
import org.mos.p22p.model.P22P.PSJoin
import org.mos.p22p.utils.Config
import org.mos.p22p.utils.LogHelper

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.outils.serialize.SessionIDGenerator
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import java.util.Random
import org.mos.p22p.Daos
import java.util.concurrent.ConcurrentLinkedQueue
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import com.google.common.cache.Cache
import org.mos.mcore.tools.time.JodaTimeHelper
import org.mos.p22p.utils.PacketIMHelper
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPNodeJoin extends PSMPZP[PSJoin] {
  override def service = PZPNodeJoinService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPNodeJoinService extends LogHelper with PBUtils with LService[PSJoin] with PMNodeHelper {
  val rand = new Random(System.currentTimeMillis() / 1000 / 3600);
  val cacheByIdx = CacheBuilder.newBuilder().initialCapacity(100000)
    .expireAfterWrite(1200, TimeUnit.SECONDS)
    .maximumSize(100000)
    .concurrencyLevel(Runtime.getRuntime().availableProcessors()).build().asInstanceOf[Cache[Int, String]];

  val joinMessage = new LinkedBlockingQueue[(FramePacket, PSJoin, CompleteHandler)]();

  override def onPBPacket(pack: FramePacket, pbo: PSJoin, handler: CompleteHandler) = {
    //    log.debug("JoinService::" + pack.getFrom() + ",OP=" + pbo.getOp)
    var ret = PRetJoin.newBuilder();
    val network = networkByID(pbo.getNid)
    var fromuid: String = null;
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      if (pbo.getIswallmessage) {
        joinMessage.offer((pack, pbo, handler));
      } else {
        bgApply(pack, pbo, handler)
      }
    }
  }
  val running = new AtomicBoolean(true);
  new Thread(new Runnable() {

    def run(): Unit = {
      while (running.get) {
        try {
          val p = joinMessage.poll(10, TimeUnit.SECONDS)
          if (p != null) {
            bgApply(p._1, p._2, p._3);
          }
        } catch {
          case t: Throwable =>
            log.error("error in join network JINPZP", t)
            Thread.sleep(3000)
        }

      }
    }
  }).start()

  def bgApply(pack: FramePacket, pbo: PSJoin, handler: CompleteHandler) = {
    //    log.debug("JoinService::" + pack.getFrom() + ",OP=" + pbo.getOp)
    var ret = PRetJoin.newBuilder();
    val network = networkByID(pbo.getNid)
    var fromuid: String = null;
    try {
      MDCSetBCUID(network)
      //       pbo.getMyInfo.getNodeName
      val from = pbo.getMyInfo;
      fromuid = from.getBcuid
      //        log.debug("verify Message=="+MessageSender.verifyMessage(pack)(network));
      ret.setMyInfo(toPMNode(network.root).setBitEncs(network.bitenc.strEnc)).setMessageid(pbo.getMessageid)
      if (StringUtils.isBlank(from.getBcuid) || StringUtils.isBlank(from.getPubKey)
        || StringUtils.isBlank(from.getUri)) {
        log.info("get empty bcuid:" + from.getBcuid + ",uri=" + from.getUri + ",trynodeid=" + from.getTryNodeIdx
          + "nodeid=" + from.getNodeIdx);
        ret.setRetCode(-2).setRetMessage("unknow id");
      } else if (from.getTryNodeIdx <= 0 && from.getNodeIdx <= 0) {
        log.info("get zero id from:" + from.getBcuid + ",uri=" + from.getUri + ",trynodeid=" + from.getTryNodeIdx
          + "nodeid=" + from.getNodeIdx);
        ret.setRetCode(-3).setRetMessage("zero id");
      } else if (!SessionIDGenerator.checkSum(from.getBcuid.substring(1))) {
        log.error("invalid bcuid:" + from.getBcuid);
        ret.setRetCode(-4).setRetMessage("bcuid invalid");
        MessageSender.dropNode(from.getBcuid)
      } else if (network.joinNetwork.blackList.containsKey(from.getBcuid)) {
        log.error("black current bcuid:" + from.getBcuid)
        ret.setRetCode(-5).setRetMessage("bcuid in black list:" + from.getBcuid);
        MessageSender.dropNode(from.getBcuid)
      } else if (pbo.getOp == PSJoin.Operation.NODEID_REQUEST) {

        var tryidx = 0;
        var cc = 0;
        cacheByIdx.synchronized {
          do {
            tryidx = Math.abs(rand.nextInt() % Daos.props.get("otrans.node.max_nodes", 256));
            val n1 = network.directNodes.filter({ _pn =>
              _pn.node_idx == tryidx
            })
            val n2 = network.pendingNodes.filter { _pn =>
              _pn.node_idx == tryidx || _pn.try_node_idx == tryidx
            }
            if (n1.size + n2.size > 0) {
              tryidx = 0;
              cc = cc + 1;
            }
          } while (tryidx == 0 || cacheByIdx.getIfPresent(tryidx) != null);

          cacheByIdx.put(tryidx, from.getUri);
        }
        ret.setRetCode(0).setRetMessage("ok")
        log.info("set try index:" + from.getUri + ",nodeidx=" + from.getNodeIdx + ",tryIdx=" + from.getTryNodeIdx + ",bcuid=" + from.getBcuid
          + ",tryidx==>" + tryidx)
        ret.setRetTryIdx(tryidx).setTryCount(cc).setStrbits(network.bitenc.strEnc)
        //          ret.setRetCode(value)
      } else if (pbo.getOp == PSJoin.Operation.NODE_CONNECT) {
        System.setProperty("java.protocol.handler.pkgs", "org.csc.bcapi.url");
        //          log.info("getConnection:" + from.getUri + ",nodeidx=" + from.getNodeIdx + ",tryIdx=" + from.getTryNodeIdx + ",bcuid=" + from.getBcuid)
        //          from.getUri.split(",").map { new URL(_) } //checking uri
        //          val _urlcheck = new URL(from.getUri)
        val samenode = StringUtils.equals(
          pbo.getNetworkInstance,
          Networks.instanceid);
        if (samenode &&
          ((from.getTryNodeIdx > 0 && from.getTryNodeIdx == network.root().node_idx) ||
            StringUtils.equals(from.getBcuid, network.root().bcuid))) {
          if (!StringUtils.equals(from.getBcuid, network.root().bcuid)) {
            log.debug("same NodeIdx :" + from.getNodeIdx + ",tryIdx=" + from.getTryNodeIdx + ",bcuid=" + from.getBcuid + ",netid=" +
              samenode + ":" + pbo.getNetworkInstance + "-->" + Networks.instanceid);
            //            MessageSender.dropNode(from.getBcuid);
          }
          ret.setRetCode(-1)
          throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
        } else if (network.node_bits.testBit(from.getTryNodeIdx)) {
          network.nodeByIdx(from.getTryNodeIdx) match {
            case Some(n) if n.bcuid.endsWith(from.getBcuid) && !samenode && from.getTryNodeIdx != network.root().node_idx =>
              log.debug("node back online ")
              ret.setRetCode(2).setRetTryIdx(n.node_idx)
            //              network.onlineMap.put(n.bcuid, n);
            case _ =>
              ret.setRetCode(-2)
              log.info("nodebits duplicated NodeIdx directnode:" + from.getNodeIdx);
              throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
          }
        } else {
          //name, idx, protocol, address, port, startup_time, pub_key, counter,idx
          val ccpending = network.pendingNodes.filter { n =>
            JodaTimeHelper.secondIntFromNow(n.startup_time) < Config.TIMEOUT_SEC_PENDING_CONFIRM && StringUtils.equalsIgnoreCase(n.uri, from.getUri) && n.try_node_idx != from.getTryNodeIdx ||
              JodaTimeHelper.secondIntFromNow(n.startup_time) < Config.TIMEOUT_SEC_PENDING_CONFIRM && StringUtils.equalsIgnoreCase(n.uri, from.getUri) && n.bcuid != from.getBcuid ||
              n.try_node_idx == from.getTryNodeIdx && !from.getBcuid.equals(n.bcuid) && !StringUtils.equals(from.getPubKey, n.pub_key)
          }.size
          val ccdnodes = network.directNodes.filter { n =>
            StringUtils.equalsIgnoreCase(n.uri, from.getUri) && (n.node_idx > 0 && n.node_idx != from.getNodeIdx) ||
              StringUtils.equalsIgnoreCase(n.uri, from.getUri) && (n.try_node_idx > 0 && n.try_node_idx != from.getNodeIdx) ||
              StringUtils.equalsIgnoreCase(n.uri, from.getUri) && n.bcuid != from.getBcuid ||
              n.node_idx == from.getTryNodeIdx && !from.getBcuid.equals(n.bcuid) && !StringUtils.equals(from.getPubKey, n.pub_key)
          }.size
          if (ccdnodes + ccpending > 0) {
            ret.setRetCode(-2)
            log.info("nodebits duplicated NodeIdx pending :idx=" + from.getNodeIdx + ",tryidx=" + from.getTryNodeIdx + ",uri=" + from.getUri + ",ccp=" + ccpending + ",ccd=" + ccdnodes
              + ",bcuid=" + from.getBcuid);
            throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
          } else {
            val n = fromPMNode(from);
            log.debug("add Pending Node:bcuid=" + n.bcuid + ",tryidx=" + from.getTryNodeIdx);
            network.onlineMap.put(n.bcuid, n);
            network.addPendingNode(n)
            val allNC = (network.directNodeByBcuid.size +
              network.pendingNodeByBcuid.size)
            if (pbo.getNodeCount >= allNC * 2 / 3
              && (pbo.getNodeNotifiedCount >= pbo.getNodeCount - 1)) {
              log.debug("start join network and vote")
              network.voteNodeMap.runOnce();
            } else {
              log.debug("cannot start join network and vote:NC=" + pbo.getNodeCount + ",all=" + allNC)
            }
          }
        }
      } else if (pbo.getOp == PSJoin.Operation.NODE_CONNECT) {
        //        NodeInstance.curnode.addPendingNode(new LinkNode(from.getProtocol, from.getNodeName, from.getAddress, //
        //          from.getPort, from.getStartupTime, from.getPubKey, from.getTryNodeIdx, from.getNodeIdx))
      }

      //      ret.addNodes(toPMNode(NodeInstance.root));

      network.directNodes.map { _pn =>
        //          log.debug("direct.node==" + _pn.bcuid)
        ret.addNodes(toPMNode(_pn));
      }
      network.pendingNodes.map { _pn =>
        //          log.debug("pending.node==" + _pn.bcuid)
        ret.addNodes(toPMNode(_pn));
      }
    } catch {
      case fe: NodeInfoDuplicated => {
        ret.setMyInfo(toPMNode(network.root).setBitEncs(network.bitenc.strEnc))
        ret.addNodes(toPMNode(network.root));
      }
      case e: FBSException => {
        ret.clear()
        ret.setRetCode(-2).setRetMessage("" + e.getMessage)
      }
      case t: Throwable => {
        log.error("error:", t);
        ret.clear()
        ret.setRetCode(-3).setRetMessage("" + t.getMessage)
      }
    } finally {
      if (network != null && fromuid != null && pbo.getIswallmessage) {
        network.postMessage("RJIPZP", Left(ret.build()), ret.getMessageid, fromuid)
      }
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.JIN.name();
}
