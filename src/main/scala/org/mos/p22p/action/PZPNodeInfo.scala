package org.mos.p22p.action

import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.p22p.PSMPZP
import org.mos.p22p.exception.FBSException
import org.mos.p22p.exception.NodeInfoDuplicated
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PRetNodeInfo
import org.mos.p22p.model.P22P.PSNodeInfo
import org.mos.p22p.utils.LogHelper

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.mos.p22p.model.P22P.PRetNodeInfo.PLocInfo

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPNodeInfo extends PSMPZP[PSNodeInfo] {
  override def service = PZPNodeInfoService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPNodeInfoService extends LogHelper with PBUtils with LService[PSNodeInfo] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeInfo, handler: CompleteHandler) = {
    //    log.debug("onPBPacket::" + pbo)
    var ret = PRetNodeInfo.newBuilder();
    val network = if (pbo == null || StringUtils.isBlank(pbo.getNid)) { null }
    else {
      networkByID(pbo.getNid)
    }
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network);
        //       pbo.getMyInfo.getNodeName
        ret.setCurrent(toPMNode(network.root()))
        val pending = network.pendingNodes;
        val directNodes = network.directNodes;
        //        log.debug("pending=" + network.pendingNodes.size + "::" + network.pendingNodes)
        //      ret.addNodes(toPMNode(NodeInstance.curnode));
        pending.map { _pn =>
          log.debug("pending==" + _pn)
          ret.addPnodes(toPMNode(_pn));
        }
        directNodes.map { _pn =>
          log.debug("directnodes==" + _pn)
          if (_pn.bcuid.equals(network.root().bcuid)) {
            ret.addDnodes(toPMNode(network.root()));
          } else {
            ret.addDnodes(toPMNode(_pn));
          }
        }
        network.nodesByLocID.map(kv => {
          val locid = kv._1
          val nodes = kv._2._2
          val info=PLocInfo.newBuilder().setLocId(locid)
          nodes.forEach(node=>{
            info.setLocGwuris(node.loc_gwuris)
            info.addLocNodebcuids(node.bcuid)
          })
          ret.addLocinfs(info)
        })
        ret.setBitEncs(network.node_strBits);
      } catch {
        case fe: NodeInfoDuplicated => {
          ret.clear();
          ret.setRetCode(-1).setRetMessage(fe.getMessage)
        }
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.INF.name();
}
