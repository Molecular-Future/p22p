package org.mos.p22p.pbft

import onight.oapi.scala.traits.OLog
import onight.tfw.outils.serialize.SessionIDGenerator
import org.mos.mcore.crypto.BitMap

import scala.collection.immutable.Map

object DMVotingNodeBits extends Votable with OLog with PMNodeHelper with BitMap with LogHelper {
  def makeDecision(network: Network, pbo: PVBase, reallist: Map[String,PVBase] = null): Option[String] = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    MDCSetMessageID(pbo.getMessageUid)
    log.debug("makeDecision NodeBits:F=" + pbo.getFromBcuid + ",R=" + vb.getNodeBitsEnc + ",S=" + pbo.getState
      + ",V=" + pbo.getV + ",J=" + pbo.getRejectState);
    if (pbo.getRejectState == PBFTStage.REJECT) {
      None;
    } else {

      val encbits = mapToBigInt(vb.getNodeBitsEnc);
      val pendingbits = mapToBigInt(vb.getPendingBitsEnc);
      val oldtotalbits = encbits;
      var totalbits = encbits;
      val pendingInList = vb.getPendingNodesList.asScala.filter { pn =>
        pn.getBcuid.equals(network.root().bcuid) ||
          network.pendingNodeByBcuid.contains(pn.getBcuid) ||
          //          network.onlineMap.contains(pn.getBcuid) ||
          network.directNodeByBcuid.contains(pn.getBcuid)
      }
      totalbits = totalbits.clearBit(network.root().try_node_idx)
      network.directNodes.map { pn =>
        if (SessionIDGenerator.checkSum(pn.bcuid.substring(1))) {
          log.debug("directnode idx=" + pn.node_idx + ",bcuid=" + pn.bcuid);
          totalbits = totalbits.clearBit(pn.node_idx)
        } else {
          log.debug("directnode bcuid checksum error=nodeidx=" + pn.node_idx + ",bcuid=" + pn.bcuid);
        }
      }
      network.pendingNodes.map { pn =>
        //      if(!Networks.instance.onlineMap.contains(pn.bcuid)){
        //        log.warn("pending node not online:"+pn.bcuid);
        //      }
        if (SessionIDGenerator.checkSum(pn.bcuid.substring(1))) {
          log.debug("pending tryidx=" + pn.try_node_idx + ",bcuid=" + pn.bcuid);
          totalbits = totalbits.clearBit(pn.try_node_idx)
        } else {
          log.debug("pendingnode bcuid checksum error=trynodeidx=" + pn.try_node_idx + ",bcuid=" + pn.bcuid);
        }
      }
      log.debug("totalbits::" + oldtotalbits.toString(16) + "-->" + totalbits.toString(16)
        + ":pbpendinCount=" + vb.getPendingNodesCount
        + ":pbInListCount=" + pendingInList.size + ":" + vb.getNodeBitsEnc
        + ":tb=" + totalbits.bitCount
        + ":pendingInList::" + pendingInList.foldLeft("")((A, p) => A + p.getBcuid + ","))

      //1. check encbits. for direct nodes 
      if (pendingInList.size == vb.getPendingNodesCount
        && totalbits.bitCount == 0) {
        log.debug("Make_Decision:" + vb.getNodeBitsEnc + ":");
        Some(vb.getNodeBitsEnc)
      } else {
        log.debug("reject for node_bits not equals:")
        None;
      }
    }
  }
  def finalConverge(network: Network, pbo: PVBase): Unit = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    log.debug("FinalConverge! for DMVotingNodeBits:F=" + pbo.getFromBcuid + ",Result=" + vb.getNodeBitsEnc);

    val nodes = vb.getPendingNodesList.asScala.map { n =>
      fromPMNode(n)
    }.toList

    val encbits = mapToBigInt(vb.getNodeBitsEnc);
    val hasBitExistsInMypending = network.pendingNodes.map { n =>
      if (encbits.testBit(n.try_node_idx)) {
        network.addDNode(n);
      }
    }
    network.pending2DirectNode(nodes) match {
      case true =>
        log.info("success add pending to direct nodes::" + nodes.size)
      case false =>
        log.warn("failed add pending to direct nodes::" + nodes.size)
    }

  }
}
