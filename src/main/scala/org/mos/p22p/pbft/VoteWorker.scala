package org.mos.p22p.pbft

import onight.tfw.outils.serialize.UUIDGenerator
import org.apache.commons.lang3.StringUtils

import scala.language.implicitConversions

case class VoteWorker(network: Network, voteQueue: VoteQueue) extends SRunner with LogHelper with PMNodeHelper {

  def getName() = "VoteWorker"

  def STR_seq(pbo: PVBaseOrBuilder): String = STR_seq(pbo.getMTypeValue, pbo.getExtType match {
    case null      => ""
    case v: String => v
  })

  def STR_seq(uid: Int, extstr: String = ""): String = "v_seq_" + network.netid + "." + uid

  def wallMessage(network: Network, pbo: PVBase) = {

    if (pbo.getRejectState == PBFTStage.REJECT) {
      network.wallOutsideMessage("VOTPZP", Left(pbo), pbo.getMessageUid, '8')
    } else {
      network.wallMessage("VOTPZP", Left(pbo), pbo.getMessageUid, '8')
    }
  }

  def voteViewChange(network: Network, pbo1: PVBase) = {
    log.info("try start voteViewChange");
    if (StringUtils.equals(pbo1.getOriginBcuid, network.root().bcuid)) {

      val vbase = PVBase.newBuilder();
      vbase.setV(pbo1.getV + 1)
      vbase.setN(pbo1.getN)
      vbase.setMType(PVType.VIEW_CHANGE).setStoreNum(pbo1.getStoreNum + 1).setViewCounter(0)
      vbase.setMessageUid(UUIDGenerator.generate())
      vbase.setNid(network.netid)
      vbase.setOriginBcuid(network.root().bcuid)
      vbase.setFromBcuid(network.root.bcuid);
      vbase.setCreateTime(System.currentTimeMillis())
      vbase.setLastUpdateTime(vbase.getCreateTime)
      vbase.setContents(toByteString(PBVoteViewChange.newBuilder().setStoreNum(pbo1.getStoreNum + 1)
        .setViewCounter(0).setV(vbase.getV)));
      val ov = Daos.viewstateDB.get(network.stateStorage.STR_seq(vbase)) match {
        case ov if ov == null =>
          null
        case ov if ov != null =>
          val pbdb = PVBase.newBuilder().mergeFrom(ov)
          if (System.currentTimeMillis() - pbdb.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE) {
            null
          } else {
            ov
          }
        case _ =>
          null;
      }
      if (ov != null) {
        Daos.viewstateDB.put(network.stateStorage.STR_seq(vbase), vbase.setState(PBFTStage.PRE_PREPARE).build())
        voteQueue.appendInQ(vbase.setState(PBFTStage.PENDING_SEND).build())
      }
    }
  }
  def makeVote(pbo: PVBase, ovbb: PVBase, newstate: PBFTStage) = {

    val ov = if (ovbb != null) ovbb.toBuilder().build() else PVBase.newBuilder().build()
    MDCSetMessageID(pbo.getMTypeValue + "|" + pbo.getMessageUid)
    val reply = pbo.toBuilder().setState(newstate)
      .setFromBcuid(network.root().bcuid)
      .setOldState(pbo.getState);
    if (pbo.getState == PBFTStage.PENDING_SEND) {
      log.debug("PendingSend=" + pbo.getState + ",trystate=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);
      wallMessage(network, reply.build());
    } else {
      implicit val dm = pbo.getMType match {
        case PVType.NETWORK_IDX =>
          DMVotingNodeBits
        case PVType.VIEW_CHANGE =>
          DMViewChange
        case PVType.STR_PBFT_VOTE =>
          ExtStringVoter
        case _ => null;
      }
      log.debug("makeVote:State=" + pbo.getState + ",trystate=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);

      newstate match {
        case PBFTStage.PRE_PREPARE =>
          log.debug("Vote::Move TO Next=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",=O=" + pbo.getOriginBcuid);

          wallMessage(network, reply.build());
        //        PBFTStage.PREPARE
        //        case PBFTStage.REJECT =>
        //          reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
        //          //        log.info("MergeSuccess.Local!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
        //          val dbkey = network.stateStorage.STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + pbo.getState;
        //
        //          Daos.viewstateDB.get(dbkey.getBytes).get match {
        //            case ov if ov != null => //&& PVBase.newBuilder().mergeFrom(ov.getExtdata).getState == newstate =>
        //              log.debug("Omit duplicated=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
        //              PBFTStage.DUPLICATE;
        //            case _ =>
        //              if (network.isLocalNode(pbo.getOriginBcuid)) {
        //                log.debug("omit reject Message for local:" + pbo.getFromBcuid);
        //              } else {
        //                wallMessage(network, reply.build());
        //              }
        //          }
        case _ =>
          network.stateStorage.makeVote(pbo, ov, newstate) match {
            case PBFTStage.PREPARE | PBFTStage.COMMIT => //|| s == PBFTStage.REPLY =>
              log.debug("Vote::Move TO Next,State=" + reply.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);
              wallMessage(network, reply.build())
            case PBFTStage.REJECT =>
              log.debug("Vote::Reject =" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid);
              reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
              if (network.isLocalNode(pbo.getOriginBcuid)) {
                log.debug("omit reject Message afterVote for local:" + pbo.getOriginBcuid);
                //              } else if (pbo.getRejectState == PBFTStage.REJECT) {
                //                log.debug("omit reject Message for remote:" + pbo.getOriginBcuid);
              } else {
                wallMessage(network, reply.build());
                //                MessageSender.replyPostMessage("VOTPZP", pbo.getFromBcuid, reply.build());
              }
            case PBFTStage.REPLY =>

              network.stateStorage.saveStageV(pbo, ov);
              log.info("MergeSuccess." + pbo.getMType + ":V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",OF=" + pbo.getOriginBcuid)
              if (dm != null) {
                dm.finalConverge(network, pbo);
              }

              if (pbo.getViewCounter >= Config.NUM_VIEWS_EACH_SNAPSHOT && pbo.getMType != PVType.VIEW_CHANGE) {
                voteViewChange(network, pbo);
              }
            case PBFTStage.DUPLICATE =>
            //              log.info("Duplicated Vote Message!:V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",State=" + pbo.getState + ",org=" + pbo.getOriginBcuid)
            case s @ _               =>
            //              log.debug("Noop for state:" + newstate + ",voteresult=" + s)

          }
      }
    }
  }
  def runOnce() = {
    try {
      MDCSetBCUID(network);
      //      var (pbo, ov, newstate) = VoteQueue.pollQ();
      var hasWork = true;
      log.debug("Get Q size=" + voteQueue.inQ.size())
      while (hasWork) {
        val q = voteQueue.pollQ();
        if (q == null) {
          hasWork = false;
        } else {
          makeVote(q._1, q._2, q._3)
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("unknow Error:" + e.getMessage, e)
    } finally {
      MDCRemoveMessageID()

    }
  }
}