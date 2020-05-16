package org.mos.p22p.tasks

import java.net.URL
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.outils.serialize.SessionIDGenerator
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.Map

//投票决定当前的节点
case class CheckingHealthy(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "CheckingHealthy"
  val checking = new AtomicBoolean(false)
  val failedChecking = Map[String, AtomicInteger]();
  val lastNodesCheckTime =new ConcurrentHashMap[String, Long]()
  def runOnce() = {
    if (checking.compareAndSet(false, true)) {
      try {
        MDCSetBCUID(network)

        //    if (!StringUtils.isBlank(network.root().pub_key)) {
        val pack = PSNodeInfo.newBuilder().setNode(toPMNode(network.root()))
          .setNid(network.netid).build()
        implicit val _net = network
        val pn = network.pendingNodes.filter { _.bcuid != network.root().bcuid }
        val dn = network.directNodes.filter { _.bcuid != network.root().bcuid }
        val cdl = new CountDownLatch(pn.size + dn.size);
        val sendCounter = new AtomicInteger(0);
        pn.map { n =>
          try {
            if (sendCounter.incrementAndGet() > 21) {
              log.info("not send hbt ,counter to large");
              cdl.countDown();
            } else if (StringUtils.isBlank(n.bcuid) || n.bcuid.length() <= 2 || !SessionIDGenerator.checkSum(n.bcuid.substring(1)) || StringUtils.isBlank(n.uri)
              || StringUtils.isBlank(n.pub_key)) {
              cdl.countDown();
              log.error("DropNode From Pending ERROR BCUID CheckSum:bcuid=" + ",n=" + n.bcuid + ",uri=" + n.uri + ";");
              network.removePendingNode(n);
              network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
              network.joinNetwork.pendingJoinNodes.remove(n.bcuid)
              MessageSender.dropNode(n.bcuid)
              sendCounter.decrementAndGet();
            } else {
              val lastCheckTime = lastNodesCheckTime.get(n.bcuid)
              if (lastCheckTime != null && System.currentTimeMillis() - lastCheckTime.longValue() < Config.TICK_CHECK_HEALTHY * 3) {
                sendCounter.decrementAndGet();
                cdl.countDown();
              } else {
                lastNodesCheckTime.put(n.bcuid, System.currentTimeMillis());
                log.debug("checking Health to pending@" + n.bcuid + ",uri=" + n.uri)
                MessageSender.asendMessage("HBTPZP", pack, n, new CallBack[FramePacket] {
                  def onSuccess(fp: FramePacket) = {
                    cdl.countDown()
                    MDCSetBCUID(network)

                    log.debug("send HBTPZP success:to uri=" + n.uri + ",bcuid=" + n.bcuid)
                    failedChecking.remove(n.bcuid + "," + n.startup_time)
                    val retpack = PRetNodeInfo.newBuilder().mergeFrom(fp.getBody);

                    //          log.debug("get nodes:" + retpack);
                    if (retpack.getRetCode() == 0 && retpack.hasCurrent()) {
                      if (retpack.getCurrent == null) {
                        log.debug("Node EROR NotFOUND:" + retpack);
                        network.removePendingNode(n);
                      } else if (!StringUtils.equals(retpack.getCurrent.getBcuid, n.bcuid)) {
                        log.warn("Node EROR BCUID Not Equal:" + retpack.getCurrent.getBcuid + ",n=" + n.bcuid
                          + ",nodename=+" + retpack.getCurrent.getNodeName + ",uri=" + retpack.getCurrent.getUri
                          + ",try_node_idx=" + retpack.getCurrent.getTryNodeIdx + ",nodeidx=" + retpack.getCurrent.getNodeIdx
                          + ",pnodesize=" + retpack.getPnodesCount + ",dnodesize=" + retpack.getDnodesCount
                          + ",bit_encs=" + retpack.getBitEncs);
                        network.removePendingNode(n);
                        network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                        network.joinNetwork.pendingJoinNodes.remove(n.bcuid)
                        MessageSender.dropNode(n.bcuid)
                      } else {
                        log.debug("get nodes:pendingcount=" + retpack.getPnodesCount + ",dnodecount=" + retpack.getDnodesCount);
                        network.onlineMap.put(n.bcuid, n);
                        def joinFunc(pn: PMNodeInfo) = {
                          val pnode = fromPMNode(pn);
                          network.addPendingNode(pnode);
                          network.joinNetwork.pendingJoinNodes.put(pnode.bcuid, pnode)
                        }
                        retpack.getPnodesList.map(joinFunc)
                        retpack.getDnodesList.map(joinFunc)
                      }
                    } else {
                      log.warn("hbt get empty pn=" + n + ",pack:" + retpack);
                    }
                  }
                  def onFailed(e: java.lang.Exception, fp: FramePacket) {
                    cdl.countDown()
                    MDCSetBCUID(network)

                    log.warn("send HBTPZP ERROR uri=" + n.uri + ",bcuid=" + n.bcuid + ",startup=" + n.startup_time + ",e=" + e.getMessage, e)
                    failedChecking.get(n.bcuid + "," + n.startup_time) match {
                      case Some(cc) =>
                        if (cc.incrementAndGet() >= Config.HB_FAILED_COUNT) {
                          log.warn("Drop Node for HeatBeat Failed!uri=" + n.uri + ",bcuid=" + n.bcuid);
                          network.removePendingNode(n);
                          MessageSender.dropNode(n)
                          network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                          network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
                        } else {
                          log.warn("PNode warning. HeatBeat Feiled!:failedcc=" + cc.get + ",uri=" + n.uri + ",bcuid=" + n.bcuid, e);
                        }

                      case None =>
                        failedChecking.put(n.bcuid + "," + n.startup_time, new AtomicInteger(1));
                    }
                  }
                }, '9');
              }
            }
          } catch {
            case t: Throwable =>
              log.error("checking peningnodes error: n=" + n.uri, t);
          }

        }
        sendCounter.set(0)
        dn.map { n =>
          try {
            if (!SessionIDGenerator.checkSum(n.bcuid.substring(1))) {
              cdl.countDown();
              log.debug("DropNode From DNodes ERROR BCUID CheckSum:bcuid=" + ",n=" + n.bcuid + ",uri=" + n.uri + ";");
              MessageSender.dropNode(n)
              network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
              network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
              network.removeDNode(n);
              network.removePendingNode(n);
            } else if (sendCounter.incrementAndGet() > 21) {
              log.info("not send hbt ,counter to large");
              cdl.countDown();
            }else {
              val lastCheckTime = lastNodesCheckTime.get(n.bcuid)
              if (lastCheckTime != null && System.currentTimeMillis() - lastCheckTime.longValue() < Config.TICK_CHECK_HEALTHY * 3) {
                sendCounter.decrementAndGet();
                cdl.countDown();
              } else {
                log.debug("checking Health to directs@" + n.bcuid + ",uri=" + n.uri)
                lastNodesCheckTime.put(n.bcuid, System.currentTimeMillis());
                val start = System.currentTimeMillis();
                MessageSender.asendMessage("HBTPZP", pack, n, new CallBack[FramePacket] {
                  def onSuccess(fp: FramePacket) = {
                    cdl.countDown()
                    MDCSetBCUID(network)

                    log.debug("send HBTPZP Direct success:to " + n.uri + ",bcuid=" + n.bcuid)
                    network.onlineMap.put(n.bcuid, n);
                    failedChecking.remove(n.bcuid + "," + n.startup_time)
                    val retpack = PRetNodeInfo.newBuilder().mergeFrom(fp.getBody);
                    log.debug("get nodes:pendingcount=" + retpack.getPnodesCount + ",dnodecount=" + retpack.getDnodesCount);
                    if (retpack.getRetCode() == 0 && retpack.hasCurrent()) {
                      log.debug("get successful hbtpzp, dn=" + n);
                      if (retpack.getCurrent == null) {
                        log.debug("Node EROR NotFOUND:" + retpack);
                        network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
                        network.removeDNode(n);
                      } else if (!StringUtils.equals(retpack.getCurrent.getBcuid, n.bcuid)) {
                        log.warn("Node EROR BCUID Not Equal:" + retpack.getCurrent.getBcuid + ",n=" + n.bcuid);
                        network.removeDNode(n);
                        network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                        network.joinNetwork.pendingJoinNodes.remove(n.bcuid)
                        MessageSender.dropNode(n.bcuid)

                      } else {
                        retpack.getPnodesList.map { pn =>
                          network.addPendingNode(fromPMNode(pn));
                        }
                        //fix bugs when some node down.2018.3
                        retpack.getDnodesList.map { pn =>
                          if (network.nodeByBcuid(pn.getBcuid) == network.noneNode) {
                            network.addPendingNode(fromPMNode(pn));
                          }
                        }
                      }
                    } else {
                      log.warn("hbt get empty dn=" + n + ",pack:" + retpack);

                    }
                  }
                  def onFailed(e: java.lang.Exception, fp: FramePacket) {
                    cdl.countDown()
                    MDCSetBCUID(network)

                    log.warn("send HBTPZP Direct ERROR " + n.uri + ",startup=" + n.startup_time + ",cost=" + (System.currentTimeMillis() - start) + ",e=" + e.getMessage, e)
                    failedChecking.get(n.bcuid + "," + n.startup_time) match {
                      case Some(cc) =>
                        if (cc.incrementAndGet() >= Config.HB_FAILED_COUNT) {
                          log.warn("Drop DNode for HeatBeat Failed!cc=" + cc + ",uri=" + n.uri + ",bcuid=" + n.bcuid);
                          MessageSender.dropNode(n)
                          network.joinNetwork.joinedNodes.remove(n.uri.hashCode());
                          network.joinNetwork.pendingJoinNodes.remove(n.bcuid);
                          network.removeDNode(n);
                          network.removePendingNode(n);
                        } else {
                          log.warn("DNode warning. HeatBeat Feiled!:failedcc=" + cc.get + ",uri=" + n.uri + ",bcuid=" + n.bcuid, e);
                        }
                      case None =>
                        failedChecking.put(n.bcuid + "," + n.startup_time, new AtomicInteger(1));
                    }
                  }

                }, '9');
              }
            }
          } catch {
            case t: Throwable =>
              log.error("checking dnodes error:" + n, t);
          }
        }

        try {
          cdl.await(Math.min(Config.TICK_CHECK_HEALTHY, 10), TimeUnit.SECONDS)
        } catch {
          case t: Throwable =>
            log.debug("checking Health wait error:" + t.getMessage, t);
        }

      } finally {
        checking.compareAndSet(true, false);
      }
    }
    //    }
  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
  def main(args: Array[String]): Unit = {
    //System.setProperty("java.protocol.handler.pkgs", "org.csc.bcapi.url");
    println(new URL("tcp://127.0.0.1:5100").getHost);
  }
}