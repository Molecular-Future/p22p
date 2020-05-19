package org.mos.p22p.tasks

import java.util.HashMap
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, LinkedBlockingQueue, TimeUnit}

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.outils.serialize.UUIDGenerator
import org.apache.commons.lang3.StringUtils
import org.mos.mcore.crypto.BitMap

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.math.BigInt

//投票决定当前的节点
case class JoinNetwork(network: Network, statupNodes: Iterable[PNode]) extends SRunner with PMNodeHelper with LogHelper with BitMap {
  def getName() = "JoinNetwork"
  val sameNodes = new HashMap[Integer, PNode]();
  val pendingJoinNodes = new ConcurrentHashMap[String, PNode]();
  val idTryNodes = new ConcurrentHashMap[String, PRetJoinOrBuilder]();
  val joinedNodes = new HashMap[Integer, PNode]();
  val duplictedInfoNodes = Map[Int, PNode]();
  val startupDBLoadNodes = List[PMNodeInfo]();
  val blackList = new HashMap[String, String]();
  var namedNodes: Iterable[PNode] = List[PNode]();
  val DB_KEY = ("PZP__JoinNetwork__" + network.netid).getBytes

  def syncDB() {
    val pmb = PMJoinedNode.newBuilder()
    blackList.map(kvs =>
      pmb.addBlockBcuids(kvs._1 + "==>" + kvs._2))
    joinedNodes.map(f =>
      pmb.addJoinedNodes(toPMNode(f._2)))
    Daos.odb.put(
      DB_KEY,
      pmb.build().toByteArray())
  }
  def init() {
    loadFromDB()
  }
  var hasHistory = false;
  def loadFromDB() {
    val v = Daos.odb.get(DB_KEY)
    if (v == null || v.get() == null) {
      log.debug("none block list or joined nodes");
      hasHistory = false;
    } else {
      val pmb = PMJoinedNode.newBuilder().mergeFrom(v.get)
      log.debug("load joined nodes info:" +
        pmb.getJoinedNodesList.foldLeft("")((a, b) => a + "," + b.getBcuid));
      log.debug("load block nodes info:" +
        pmb.getBlockBcuidsList.foldLeft("")((a, b) => a + "," + b));
      pmb.getBlockBcuidsList.map { x =>
        val kvs = x.split("==>");
        if (kvs.length == 2) {
          blackList.put(kvs(0).trim(), kvs(1).trim())
        }
      }
      pmb.getJoinedNodesList.map { pmnode =>
        startupDBLoadNodes.add(pmnode)
      }
      if (pmb.getJoinedNodesCount > 0) {
        hasHistory = true;
      }

    }
  }
  val idBackOnlineNodes = new ConcurrentHashMap[String, PRetJoinOrBuilder]();
  val recvJoinBuff = new LinkedBlockingQueue[(PNode, FramePacket)]();
  var joinMessageId = "";
  def runOnce() = {
    Thread.currentThread().setName("JoinNetwork");

    implicit val _net = network

    if (network.inNetwork() && pendingJoinNodes.size() <= 0
      && statupNodes.filter { x => joinedNodes.containsKey(x.uri.hashCode()) }.size == statupNodes.size) {
      log.debug("CurrentNode In Network:startupNodes.size=" + statupNodes.size + ",joinedNodeSize=" + joinedNodes.size());
      syncDB();
    } else {
      var hasNewNode = true;
      var joinLoopCount = 0;
      var tryIDDetected = true;
      while (hasNewNode && joinLoopCount < statupNodes.size) {
        try {
          val failedNodes = new HashMap[String, PNode]();
          hasNewNode = false;
          joinLoopCount = joinLoopCount + 1;
          MDCSetBCUID(network);
          namedNodes = (statupNodes ++ pendingJoinNodes.values()
            ++ startupDBLoadNodes.map { x => fromPMNode(x) }).filter { x =>
              StringUtils.isNotBlank(x.uri) && !sameNodes.containsKey(x.uri.hashCode()) && !joinedNodes.containsKey(x.uri.hashCode()) && //
                !network.isLocalNode(x) && !blackList.containsKey(x.bcuid())
            };

          //          namedNodes = namedNodes.slice(0, Math.min(10, namedNodes.size))

          if (startupDBLoadNodes.size > 0) {
            startupDBLoadNodes.clear();
          }
          val cdl = new CountDownLatch(namedNodes.size);
          joinMessageId = UUIDGenerator.generate();
          val joinbody = PSJoin.newBuilder().setOp(PSJoin.Operation.NODE_CONNECT).setMyInfo(toPMNode(network.root()))
            .setNid(network.netid)
            .setNetworkInstance(Networks.instanceid)
            .setNodeCount(network.pendingNodeByBcuid.size
              + network.directNodeByBcuid.size)
            .setNodeNotifiedCount(joinedNodes.size());
          if (!tryIDDetected) {
            joinbody.setOp(PSJoin.Operation.NODEID_REQUEST);
          } else {
            joinbody.setOp(PSJoin.Operation.NODE_CONNECT);
          }
          idBackOnlineNodes.clear();
          recvJoinBuff.clear();

          val offlineNodes = namedNodes.filter(p => { 
            !network.directNodeByBcuid.containsKey(p._bcuid) && !network.pendingNodeByBcuid.containsKey(p._bcuid) && 
            network.directNodeByBcuid.filter(n=>StringUtils.equalsIgnoreCase(n._2.uri,p.uri())).size == 0 && network.pendingNodeByBcuid.filter(n=>StringUtils.equalsIgnoreCase(n._2.uri,p.uri())).size == 0
//            
           })
          network.wallMessage("JINPZP", Left(joinbody.build()), joinMessageId);

          offlineNodes.map { n => //for each know Nodes
            //          val n = namedNodes(0);
            log.info("JoinNetwork :Run----Try to Join :MainNet=" + n.uri + ",M.bcuid=" + n.bcuid() + ",cur=" + network.root.uri);
            if (!network.root.equals(n)) {
//              MessageSender.postMessage("JINPZP", Left(joinbody.build()), n)
              MessageSender.asendMessage("JINPZP", joinbody.clearMessageid().build(), n, new CallBack[FramePacket] {
                def onSuccess(fp: FramePacket) = {
                  recvJoinBuff.offer((n, fp));
                  cdl.countDown();
                }
                def onFailed(e: java.lang.Exception, fp: FramePacket) {
                  failedNodes.put(n.uri, n);
                  log.info("send JINPZP ERROR " + n.uri + ",e=" + e.getMessage, e)
                  cdl.countDown()
                }
              });
            } else {
              cdl.countDown()
              log.debug("JoinNetwork :Finished ---- Current node is MainNode");
            }
          }
          val startWait = System.currentTimeMillis()
          while (recvJoinBuff.size() > 0 || cdl.getCount > 0 && System.currentTimeMillis() - startWait < 60 * 1000) {
            var recv = recvJoinBuff.poll(10, TimeUnit.SECONDS);
            val starttime = System.currentTimeMillis();
            if (recv != null) {
              try {
                val fp = recv._2
                val n = recv._1;
                if (fp.getBody == null || fp.getBody.length <= 10) {
                  log.info("send JINPZP warn:not ready " + n.uri + ",cost=" + (System.currentTimeMillis() - starttime))
                } else {
                  log.debug("send JINPZP success:to " + n.uri + ",cost=" + (System.currentTimeMillis() - starttime))
                  val retjoin = PRetJoin.newBuilder().mergeFrom(fp.getBody);
                  if (retjoin.getRetCode() == -1) { //same message
                    log.error("get Same Node:" + n.getName + ",n.uri=" + n.uri);
                    sameNodes.put(n.uri.hashCode(), n);
                    val newN = fromPMNode(retjoin.getMyInfo)
                    MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                    MessageSender.dropNode(n)
                    network.onlineMap.put(newN.bcuid(), newN)
                    network.addPendingNode(newN);
                  } else if (retjoin.getRetCode() == -2) {
                    log.error("get duplex NodeIndex:" + n.getName + ",tryidx=" + network.root().try_node_idx + ",idx=" + network.root().node_idx
                      + ",from.bcuid=" + n._bcuid + ",from.uri=" + n.uri());
                    sameNodes.put(n.uri.hashCode(), n);
                    duplictedInfoNodes.+=(n.uri.hashCode() -> n);
                  } else if (retjoin.getRetCode() == 2) {
                    idBackOnlineNodes.put(n.uri, retjoin);

                    joinedNodes.put(n.uri.hashCode(), n);
                    val newN = fromPMNode(retjoin.getMyInfo)
                    MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                    network.onlineMap.put(newN.bcuid(), newN)
                  } else if (retjoin.getRetCode() == 0) {
                    if (retjoin.getRetTryIdx > 0) {
                      // try idx
                      log.error("get try NodeIndex:" + n.getName + ",tryidx=" + network.root().try_node_idx + ",idx=" + network.root().node_idx
                        + ",from.bcuid=" + n._bcuid + ",from.uri=" + n.uri() + ",retid=" + retjoin.getRetTryIdx + ",retcc=" + retjoin.getTryCount);

                      idTryNodes.put(n.uri, retjoin);
                    } else {
                      joinedNodes.put(n.uri.hashCode(), n);
                      val newN = fromPMNode(retjoin.getMyInfo)
                      MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                      network.addPendingNode(newN);
                      network.onlineMap.put(newN.bcuid(), newN)
                      retjoin.getNodesList.map { node =>
                        val pnode = fromPMNode(node);
                        if (network.addPendingNode(pnode)) {
                          pendingJoinNodes.put(node.getBcuid, pnode);
                        }
                        //
                      }
                    }
                  } else {
                    log.error("unknow retcode:" + retjoin.getRetCode + ",name=" + n.getName + ",tryidx=" + network.root().try_node_idx + ",idx=" + network.root().node_idx
                      + ",from.bcuid=" + n._bcuid + ",from.uri=" + n.uri());
                  }
                  log.debug("get nodes:count=" + retjoin.getNodesCount + "," + sameNodes);
                }
              } catch {
                case t: Throwable =>
                  log.error("error in process block", t);
              }
            }
          }
          try {
            cdl.await(10, TimeUnit.SECONDS);
          } catch {
            case t: Throwable =>
              log.error("timeout error connect to all nodes:" + t.getMessage, t);
          } finally {

          }
          log.error("finished connect to all nodes:" + ",hasHistory=" + hasHistory + ",idtestsize=" + idTryNodes.size()
            + ",idBackOnlineNodes.size()=" + idBackOnlineNodes.size() + ",nameNodes.size=" + namedNodes.size
            +",offlineNodes="+offlineNodes.size);
          tryIDDetected = true;
          if (idBackOnlineNodes.size() > namedNodes.size * 2 / 3) {
            log.info("find my node back online");
            Votes.vote(idBackOnlineNodes.values().asScala.toList).PBFTVote({ p => Some(p.getMyInfo.getBitEncs) }, idBackOnlineNodes.size()) match {
              case n: Converge if n.decision != null =>
                log.info("find my node back online. converge..size=" + idBackOnlineNodes.size());
                var sugguestIdx = -1;
                idBackOnlineNodes.map(kv => {
                  val f = kv._2;
                  log.info("find my node back online. converge..nodebitenc=" + f.getMyInfo.getBitEncs + ",decision=" + n.decision.asInstanceOf[String]
                    + ",sugguestIdx=" + sugguestIdx);
                  if (StringUtils.equals(f.getMyInfo.getBitEncs, n.decision.asInstanceOf[String])) {
                    val bit = mapToBigInt(f.getMyInfo.getBitEncs)
                    sugguestIdx = f.getRetTryIdx

                    f.getNodesList.asScala.map { v =>
                      val pv = fromPMNode(v)
                      if (bit.testBit(v.getNodeIdx)) {
                        network.addDNode(pv)
                      } else {
                        network.addPendingNode(pv)
                      }

                      pendingJoinNodes.put(pv._bcuid, pv);
                    }
                  }
                })
                if (sugguestIdx > 0) {
                  network.changeNodeIdx(BigInt("0"), sugguestIdx)
                  network.addDNode(network.root())
                  log.info("node back online.success:bitenc==" + network.bitenc.strEnc + ",decision=" + n.decision.asInstanceOf[String]
                    + ",sugguestIdx=" + sugguestIdx)
                } else {
                  log.info("node back online.failed:bitenc==" + network.bitenc.strEnc + ",decision=" + n.decision.asInstanceOf[String]
                    + ",sugguestIdx=" + sugguestIdx)
                }
              case a @ _ =>
                log.info("not converge:" + a);
            }

          } else if (idTryNodes.size() > 0) {
            var idtest = 0;
            hasNewNode = true;
            var trycount = 0;
            idTryNodes.map(f => {
              if (f._2.getTryCount > trycount) {
                idtest = f._2.getRetTryIdx;
              }
            })
            joinLoopCount = 0;
            if (idtest > 0) {
              network.changeNodeIdx(BigInt("0"), idtest);
            }
            idTryNodes.clear();
            log.error("get try idx = " + idtest);
          } else if (duplictedInfoNodes.size > 0 && !network.directNodeByBcuid.contains(network.root().bcuid)) {
            //            val nl = duplictedInfoNodes.values.toSeq.PBFTVote { x => Some(x.node_idx) }
            //            nl.decision match {
            //              case Some(v: BigInteger) =>
            //log.error("duplictedInfoNodes :" + duplictedInfoNodes.size + ",nameNode=" + namedNodes.size + ",pending=" +
            //  network.pendingNodes.size + ",dnode=" +
            // network.directNodes.size + ",contains=" + network.directNodeByBcuid.contains(network.root().bcuid)
            //  + ",tryidx=" + network.root().try_node_idx + ",idx=" + network.root().node_idx);
            if ((duplictedInfoNodes.size > network.pendingNodes.size / 3
              || duplictedInfoNodes.size > network.directNodes.size / 3) && !network.directNodeByBcuid.contains(network.root().bcuid)) {
              //            val nl = duplictedInfoNodes.values.toSeq.PBFTVote { x => Some(x.node_idx) }
              //            nl.decision match {
              //              case Some(v: BigInteger) =>
              log.info("duplictedInfoNodes ,change My Index:" + duplictedInfoNodes.size + ",tryidx=" + network.root().try_node_idx + ",idx=" + network.root().node_idx);
              network.removePendingNode(network.root())
              hasNewNode = if (cdl.getCount == 0) true else false;
              joinLoopCount = 0;
              network.changeNodeIdx(duplictedInfoNodes.head._2.node_idx);
              //drop all connection first
              pendingJoinNodes.clear()
              joinedNodes.clear();
              sameNodes.clear();

              Thread.sleep(Config.TIMEOUT_SEC_PENDING_CONFIRM * 1000 * 2);

              //              case _ => {
              //                log.debug("cannot get Converage :" + nl);
              //network.changeNodeIdx();
              //              }
              //            }
              //} else {
              //network.changeNodeIdx();
            }
            //          joinedNodes.clear();
            duplictedInfoNodes.clear();
          }
          if (namedNodes.size == 0) {
            log.debug("cannot reach more nodes. try from begining :namedNodes.size=" + namedNodes.size() + ",startupNodes.size=" + statupNodes + ",joinedSize=" + joinedNodes
              + ",sameNodes=" + sameNodes.size());
            sameNodes.clear();

            // joinedNodes.clear();
            statupNodes.map { x => joinedNodes.remove(x.uri().hashCode()) }
            //next run try another index;
          } else {
            if (pendingJoinNodes.size() > 0) {
              if (pendingJoinNodes.filter(p => !failedNodes.containsKey(p._2.uri())).size > 0) {
                hasNewNode = if (cdl.getCount == 0) true else false;
              }
            }
          }
        } catch {
          case e: Throwable =>
            log.debug("JoinNetwork :Error", e);
        } finally {
          log.debug("JoinNetwork :[END]")
        }
      }
    }
  }
}