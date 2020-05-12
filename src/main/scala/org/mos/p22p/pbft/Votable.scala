package org.mos.p22p.pbft

import onight.oapi.scala.traits.OLog

import scala.collection.JavaConversions._
import scala.collection.immutable.Map

trait Votable extends OLog {
  def makeDecision(network: Network, pbo: PVBase, reallist: Map[String,PVBase] = null): Option[Any]
  def finalConverge(network: Network, pbo: PVBase): Unit
  def voteList(network: Network,pbo: PVBase, reallist: Map[String,PVBase] = null): VoteResult = { // find max store num.

    val pboresult = pbo.getState match {
      case PBFTStage.PRE_PREPARE if network.isLocalNode(pbo.getFromBcuid) =>
        return Converge(pbo.getState); //bad coding....for return
      case PBFTStage.PRE_PREPARE =>
        makeDecision(network, pbo, reallist);
      case PBFTStage.PREPARE =>
        makeDecision(network, pbo, reallist);
      case _ =>
        0
    }
    if (pbo.getState == PBFTStage.PRE_PREPARE) {
      pboresult match {
        case Some(str: String) if str.equals(Config.STR_REJECT) =>
          Converge(PBFTStage.REJECT);
        case None =>
          Converge(PBFTStage.REJECT);
        case _ =>
          Converge(pbo.getState);
      }
    } else {
      if (reallist.size < pbo.getN / 2 && pbo.getN > 4) {
        log.debug("not reach vote number ,so Undecisible")
        return Undecisible();
      }
      Votes.vote(reallist.values().toList).PBFTVote({
        x =>
          val p = PVBase.newBuilder().mergeFrom(x).build();
          val dbresult = if (pbo.getState == PBFTStage.PREPARE) {
            if (pbo.getFromBcuid.equals(p.getFromBcuid)) {
              pboresult
            } else {
              makeDecision(network: Network,p, reallist);
            }
          } else {
            0;
          }
          //          log.debug("voteNodeStages::State=" + p.getState + ",Rejet=" + p.getRejectState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
          //            + ",F=" + p.getFromBcuid + ",KEY=" + new String(x.getKey.getData.toByteArray()) + ",OVS=" + x.getValue.getSecondKey)
          if (pbo.getCreateTime - p.getCreateTime > Config.TIMEOUT_STATE_VIEW_RESET) {
            log.debug("Force TIMEOUT node state to My State:" + p.getState + ",My=" + pbo.getState);
            Some(pbo.getState)
          } else if (pbo.getV == p.getV) {
            if (p.getRejectState == PBFTStage.REJECT || dbresult != pboresult) {
              Some(PBFTStage.REJECT)
            } else {
              Some(p.getState)
            }
          } else {
            None
          }
      }, pbo.getN)
    }
  }
}

object DMViewChange extends Votable with OLog {
  def makeDecision(network: Network,pbo: PVBase, reallist: Map[String,PVBase]): Option[String] = {
    val vb = PBVoteViewChange.newBuilder().mergeFrom(pbo.getContents);
    val choise = vb.getStoreNum + "." + pbo.getV
    log.debug("makeDecision for DMViewChange:F=" + pbo.getFromBcuid + ",R=" + choise);
    val maxv = reallist.foldLeft(0)((A, kvs) => {
      val p = PVBase.newBuilder().mergeFrom(kvs._2);
      val vb = PBVoteViewChange.newBuilder().mergeFrom(p.getContents);
      Math.max(A, vb.getStoreNum)
    });
    //    val vb = PBVoteViewChange.newBuilder().mergeFrom(pbo.getContents)
    //      .setStoreNum(maxv);
    //    pbo
    Some(maxv + "." + choise)
  }
  override def voteList(network: Network,pbo: PVBase, reallist:Map[String,PVBase]): VoteResult = { // find max store num.
    if (pbo.getState == PBFTStage.PRE_PREPARE) {
      return Converge(pbo.getState);
    }
    Votes.vote(reallist.values.toList).PBFTVote({
      x =>
        val p = PVBase.newBuilder().mergeFrom(x).build();
        if (pbo.getCreateTime - p.getCreateTime > Config.TIMEOUT_STATE_VIEW_RESET) {
          Some(pbo.getState)
        } else if (pbo.getV == p.getV) {
          if (p.getRejectState == PBFTStage.REJECT) {
            Some(PBFTStage.REJECT)
          } else {
            Some(p.getState)
          }
        } else {
          None
        }
    }, pbo.getN)
  }
  
  
  def finalConverge(network: Network,pbo: PVBase): Unit = {
    val ovs = Daos.viewstateDB.listBySecondKey((network.stateStorage.STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV));
    if (ovs != null && ovs.size() > 0) {
      val reallist = ovs.filter { ov => ov._2.getStateValue == pbo.getStateValue }.toList;
      val maxv = reallist.foldLeft(0)((A, kvs) => {
        val p = PVBase.newBuilder().mergeFrom(kvs._2);
        val vb = PBVoteViewChange.newBuilder().mergeFrom(p.getContents);
        Math.max(A, vb.getStoreNum)
      });
      val vb = PBVoteViewChange.newBuilder().mergeFrom(pbo.getContents).setStoreNum(maxv);

      network.stateStorage.updateTopViewState(pbo.toBuilder()
        .setViewCounter(0)
        .setMType(PVType.NETWORK_IDX).setState(PBFTStage.REPLY)
        .setStoreNum(maxv).setContents(vb.build().toByteString()).build());
      //      dm.voteList(pbo, reallist)
      log.info("FinalConverge! for DMViewChange:F:F=" + pbo.getFromBcuid + ",Result=" + maxv + "." + vb.getStoreNum + "." + vb.getV);

    } else {
      Undecisible()
    }

  }
 

}