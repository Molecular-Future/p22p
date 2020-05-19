package org.mos.p22p.tasks

import java.util.concurrent.atomic.AtomicBoolean

import onight.tfw.outils.serialize.{SessionIDGenerator, UUIDGenerator}
import org.mos.mcore.crypto.BitMap

//投票决定当前的节点
case class VoteNodeMap(network: Network, voteQueue: VoteQueue) extends SRunner with PMNodeHelper with BitMap with LogHelper {
  def getName() = "VoteNodeMap"
  val checking = new AtomicBoolean(false)
  def runOnce() = {
    log.debug("VoteNodeMap :Run----Try to Vote Node Maps");
    val oldThreadName = Thread.currentThread().getName + ""
    if (checking.compareAndSet(false, true)) {
      try {

        Thread.currentThread().setName("VoteNodeMap");
        log.info("CurrentPNodes:PendingSize=" + network.pendingNodes.size + ",DirectNodeSize=" + network.directNodes.size);
        val vbase = PVBase.newBuilder();

        vbase.setState(PBFTStage.PENDING_SEND)
        vbase.setMType(PVType.NETWORK_IDX)
        var pendingbits = BigInt(1)
        //init. start to vote.
        if (network.joinNetwork.pendingJoinNodes.size() / 2 > network.onlineMap.size
          || network.onlineMap.size <= 0 || network.joinNetwork.pendingJoinNodes.size() + network.onlineMap.size <
          network.joinNetwork.statupNodes.size) {
          log.info("cannot vote for pendingJoinNodes Size bigger than online half:PendJoin=" +
            network.joinNetwork.pendingJoinNodes.size() + ": Online=" + network.onlineMap.size)
          //for fast load
        } else if (network.stateStorage.nextV(vbase) > 0) {

          vbase.setMessageUid(UUIDGenerator.generate())
          vbase.setOriginBcuid(network.root().bcuid)
          vbase.setFromBcuid(network.root().bcuid);
          vbase.setLastUpdateTime(System.currentTimeMillis())
          vbase.setNid(network.netid);
          val vbody = PBVoteNodeIdx.newBuilder();
          var bits = network.node_bits;
          pendingbits = BigInt(0)
          MDCSetMessageID(vbase.getMessageUid)

          network.pendingNodes.map(n =>
            //          if (network.onlineMap.contains(n.bcuid)) {
            if (n.bcuid == null || n.bcuid.length() < 2 || !SessionIDGenerator.checkSum(n.bcuid.substring(1))) {
              log.debug("error in bcuid checksum @n=" + n.name + ",bcuid=" + n.bcuid + ",try_idx==" + n.try_node_idx + ",bits=" + bits);
            } else if (bits.testBit(n.try_node_idx)) {
              log.debug("error in try_node_idx @n=" + n.name + ",try=" + n.try_node_idx + ",bits=" + bits);
            } else { //no pub keys
              pendingbits = pendingbits.setBit(n.try_node_idx);
              vbody.addPendingNodes(toPMNode(n));
              bits = bits.setBit(n.try_node_idx)
            } //          }
          )

          vbody.setPendingBitsEnc(hexToMapping(pendingbits))
          vbody.setNodeBitsEnc(hexToMapping(bits))
          vbase.setContents(toByteString(vbody))
          //      vbase.addVoteContents(Any.pack(vbody.build()))
          //      if (network.node_bits.bitCount <= 0) {
          //        log.debug("networks has not directnode!")
          log.debug("vote -- Nodes:" + vbody.getNodeBitsEnc + ",pendings=" + vbody.getPendingBitsEnc);
          vbase.setV(vbase.getV);
          vbase.setN(bits.bitCount);
          if (vbase.getN > 0) {
            log.debug("broadcast Vote Message:V=" + vbase.getV + ",N=" + vbase.getN + ",from=" + vbase.getFromBcuid
              + ",SN=" + vbase.getStoreNum + ",VC=" + vbase.getViewCounter + ",messageid=" + vbase.getMessageUid)
            val vbuild = vbase.build();
            //        Networks.wallMessage("VOTPZP", vbuild);
            voteQueue.appendInQ(vbase.setState(PBFTStage.PENDING_SEND).build())
          } else {
            log.debug("cannot start Vote N=0:")
          }
        }
        //      }

        //    NodeInstance.forwardMessage("VOTPZP", vbody.build());
        val sleepTime = if (pendingbits.bitCount > 0) {
          Math.random() * 10000000 % (Config.MAX_VOTE_SLEEP_MS - Config.MIN_VOTE_SLEEP_MS) + Config.MIN_VOTE_SLEEP_MS
        } else {
          Math.random() * 10000000 % (Config.MAX_VOTE_SLEEP_MS - Config.MIN_VOTE_SLEEP_MS) + Config.MIN_VOTE_WITH_NOCHANGE_SLEEP_MS
        }
        val plusTime = if (network.root().node_idx >= 0 && network.node_bits.bigInteger.testBit(network.root().node_idx) || network.node_bits.bigInteger.bitCount()<3) {
          Config.MIN_VOTE_SLEEP_MS + network.node_bits.bigInteger.bitCount() * 200
        } else {
          Config.MAX_VOTE_SLEEP_MS + network.node_bits.bigInteger.bitCount() * 200
        }

        val totalSleepTime = sleepTime + plusTime;
        log.info("random sleep=" + totalSleepTime);
        this.synchronized {
          this.wait(totalSleepTime.asInstanceOf[Int]);
        }
      } catch {
        case e: Throwable =>
          log.warn("unknow Error:" + e.getMessage, e)
      } finally {
        checking.compareAndSet(true, false)
        Thread.currentThread().setName(oldThreadName);
      }
    }
  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
  //  def main(args: Array[String]): Unit = {
  //    URLHelper.init()
  //    //System.setProperty("java.protocol.handler.pkgs", "org.csc.bcapi.url");
  //    println(new URL("tcp://127.0.0.1:5100").getHost);
  //  }
}