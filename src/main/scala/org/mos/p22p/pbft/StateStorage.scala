package org.mos.p22p.pbft

import onight.oapi.scala.traits.OLog
import org.apache.commons.lang3.StringUtils
import org.mos.mcore.tools.time.JodaTimeHelper

import scala.language.implicitConversions

case class StateStorage(network: Network) extends OLog {
  def STR_seq(pbo: PVBaseOrBuilder): String = STR_seq(pbo.getMTypeValue, pbo.getExtType match {
    case null => ""
    case v: String => v
  })
  def STR_seq(uid: Int, extstr: String = ""): String = "v_seq_" + network.netid + "." + uid

  def nextV(pbo: PVBase.Builder): Int = {
    this.synchronized {
      val (retv, newstate) = Daos.viewstateDB.get(STR_seq(pbo)) match {
        case ov if ov != null =>
          PVBase.newBuilder().mergeFrom(ov) match {
            case dbpbo if dbpbo.getState == PBFTStage.REPLY =>
              if (System.currentTimeMillis() - dbpbo.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE) {
                log.debug("cannot start next vote: time less than past" + dbpbo.getState + ",V=" + dbpbo.getV + ",DIS=" + JodaTimeHelper.secondFromNow(dbpbo.getLastUpdateTime));
                (-1, PBFTStage.REJECT);
              } else {
                log.debug("getting next vote:" + dbpbo.getState + ",V=" + dbpbo.getV);
                pbo.setViewCounter(dbpbo.getViewCounter + 1)
                pbo.setStoreNum(dbpbo.getStoreNum)
                (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
              }
            case dbpbo if System.currentTimeMillis() - dbpbo.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE && dbpbo.getState == PBFTStage.INIT || System.currentTimeMillis() - dbpbo.getCreateTime > Config.TIMEOUT_STATE_VIEW =>
              log.debug("recover from vote:" + dbpbo.getState + ",lastCreateTime:" + JodaTimeHelper.format(dbpbo.getCreateTime));
              pbo.setViewCounter(dbpbo.getViewCounter)
              pbo.setStoreNum(dbpbo.getStoreNum)
              getStageV(pbo.build()) match {
                case fv if fv == null =>
                  (dbpbo.getV, PBFTStage.PRE_PREPARE)
                case fv if fv != null =>
                  (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
              }
            case dbpbo =>
              log.debug("cannot start vote:" + dbpbo.getState + ",past=" + JodaTimeHelper.secondFromNow(dbpbo.getCreateTime) + ",O=" + dbpbo.getOriginBcuid);
              (-1, PBFTStage.REJECT);
          }
        case _ =>
          log.debug("New State ,db is empty");
          pbo.setViewCounter(1)
          pbo.setStoreNum(1)
          (1, PBFTStage.PRE_PREPARE);
      }
      if (Config.VOTE_DEBUG) return -1;
      if (retv > 0) {
        Daos.viewstateDB.put(STR_seq(pbo),
          pbo.setV(retv)
              .setCreateTime(System.currentTimeMillis())
              .setLastUpdateTime(System.currentTimeMillis())
              .setFromBcuid(network.root().bcuid)
              .setOriginBcuid(network.root().bcuid)
              .setState(newstate)
              .build());
      }
      retv
    }
  }

  def mergeViewState(pbo: PVBase): Option[PVBase] = {
    Daos.viewstateDB.get(STR_seq(pbo)) match {
      case ov if ov != null && StringUtils.equals(pbo.getOriginBcuid, network.root().bcuid) =>
        Some(ov) // from locals
      case ov if ov != null =>
        PVBase.newBuilder().mergeFrom(ov) match {
          case dbpbo if StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) && pbo.getV == dbpbo.getV
            && dbpbo.getStateValue <= pbo.getStateValue =>
            Some(ov);
          case dbpbo if StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) && pbo.getV == dbpbo.getV
            && dbpbo.getStateValue > pbo.getStateValue =>
            log.debug("state low dbV=" + dbpbo.getStateValue + ",pbV=" + pbo.getStateValue + ",V=" + pbo.getV + ",f=" + pbo.getFromBcuid)
            Some(ov)
          case dbpbo if (System.currentTimeMillis() - dbpbo.getLastUpdateTime > Config.TIMEOUT_STATE_VIEW)
            && pbo.getV >= dbpbo.getV  =>
            Some(ov)
          case dbpbo if pbo.getV >= dbpbo.getV && (
            dbpbo.getState == PBFTStage.INIT || dbpbo.getState == PBFTStage.REPLY
            || StringUtils.isBlank(dbpbo.getOriginBcuid)) =>
            Some(ov)
          case dbpbo if (dbpbo.getState == PBFTStage.REJECT || dbpbo.getRejectState == PBFTStage.REJECT)
            && !StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) =>
            Some(ov)
          case dbpbo @ _ =>
            pbo.getState match {
              case PBFTStage.COMMIT => //already know by other.
                log.debug("other nodes commited!")
                updateNodeStage(pbo, PBFTStage.COMMIT);
                voteNodeStages(pbo) match {
                  case n: Converge if n.decision == pbo.getState =>
                    Daos.viewstateDB.put(STR_seq(pbo),
                      pbo.toBuilder()
                          .setFromBcuid(network.root().bcuid)
                          .setState(PBFTStage.INIT)
                          .setV(pbo.getV).setStoreNum(pbo.getStoreNum).setViewCounter(pbo.getViewCounter)
                          .build())
                    None;
                  case _ =>
                    log.debug("OtherVoteCommit::NotMerge,PS=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
                    None;
                }
              case _ =>
                log.debug("Cannot MergeView For local state Not EQUAL:"
                  + "db.[O=" + dbpbo.getOriginBcuid + ",F=" + dbpbo.getFromBcuid + ",V=" + dbpbo.getV + ",S=" + dbpbo.getState
                  + ",RJ=" + dbpbo.getRejectState
                  + ",TO=" + JodaTimeHelper.secondFromNow(dbpbo.getLastUpdateTime)
                  + "],p[O=" + pbo.getOriginBcuid + ",F=" + pbo.getFromBcuid + ",V=" + pbo.getV + ",S=" + pbo.getStateValue
                  + ",RJ=" + pbo.getRejectState + "]");
                None;
            }

        }
      case _ =>
        val ov = pbo
        Some(ov)
    }
  }

  def saveStageV(pbo: PVBase, ov: PVBase):Unit={
    val key = STR_seq(pbo) + ".F." + pbo.getV;
    Daos.viewstateDB.put(key, ov);
  }

  def getStageV(pbo: PVBase):PVBase = {
    val key = STR_seq(pbo) + ".F." + pbo.getV;
    Daos.viewstateDB.get(key);
  }
  
  def updateTopViewState(pbo: PVBase) {
    this.synchronized({
      val ov = Daos.viewstateDB.get(STR_seq(pbo))
      if (ov != null) {
        Daos.viewstateDB.put(STR_seq(pbo),
            pbo.toBuilder().setLastUpdateTime(System.currentTimeMillis()).build());
      }
    })
  }

  def saveIfNotExist(pbo: PVBase, ov: PVBase, newstate: PBFTStage): PBFTStage = {
    val dbkey = STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + newstate;
    Daos.viewstateDB.get(dbkey) match {
      case dbov if dbov != null =>
        log.debug("Omit duplicated=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
        PBFTStage.DUPLICATE;
      case _ =>
        if(ov!=null)
        {
          Daos.viewstateDB.put(dbkey, ov);
        }
        newstate
    }
  }
  def makeVote(pbo: PVBase, ov: PVBase, newstate: PBFTStage)(implicit dm: Votable = null): PBFTStage = {
    val dbkey = STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV + ".";
    Daos.viewstateDB.get((dbkey + newstate)) match {
      case ov if ov != null => //&& PVBase.newBuilder().mergeFrom(ov.getExtdata).getState == newstate =>
        //        log.debug("Omit duplicated=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
        PBFTStage.DUPLICATE;
      case _ =>
        //        Daos.viewstateDB.get(dbkey + pbo.getState).get match {
        //          case ov if ov != null =>
        //            log.debug("Omit duplicated=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
        //
        //            PBFTStage.DUPLICATE;
        //          case _ =>
        voteNodeStages(pbo) match {
          case n: Converge if n.decision == pbo.getState =>
            log.debug("Vote::MergeOK,PS=" + pbo.getState + ",New=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
            Daos.viewstateDB.put(STR_seq(pbo), 
                pbo.toBuilder()
                .setState(newstate)
                .setLastUpdateTime(System.currentTimeMillis())
                .build())
            if (newstate != PBFTStage.PRE_PREPARE && newstate != PBFTStage.PREPARE) {
              saveIfNotExist(pbo, ov, newstate);
            }
            newstate

          case un: Undecisible =>
            log.debug("Vote::Undecisible:State=" + pbo.getState+",desc="+un + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
            PBFTStage.NOOP
          case no: NotConverge =>
            log.debug("Vote::Not Converge:State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
            Daos.viewstateDB.put((dbkey + pbo.getState), pbo.toBuilder()
                .setRejectState(PBFTStage.NOOP)
                .setLastUpdateTime(System.currentTimeMillis())
                .build());

            if (StringUtils.equals(pbo.getOriginBcuid, network.root().bcuid)) {
              log.debug("Reject for My Vote ")
              Daos.viewstateDB.put(STR_seq(pbo),
                pbo.toBuilder()
                    .setFromBcuid(network.root().bcuid)
                    .setState(PBFTStage.INIT)
                    .setLastUpdateTime(System.currentTimeMillis() + Config.getRandSleepForBan())
                    .build())
            }
            PBFTStage.NOOP
          case n: Converge if n.decision == PBFTStage.REJECT =>
            if (Daos.viewstateDB.get((dbkey + pbo.getState)) != null) {
               log.debug("omit reject ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
              PBFTStage.NOOP
            } else {
              Daos.viewstateDB.put((dbkey + pbo.getState), pbo.toBuilder()
                .setRejectState(PBFTStage.REJECT)
                .setLastUpdateTime(System.currentTimeMillis())
                .build());
              log.warn("getRject ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",org_bcuid=" + pbo.getOriginBcuid);
              if (StringUtils.equals(pbo.getOriginBcuid, network.root().bcuid)) {
                log.debug("Reject for this Vote ")
                Daos.viewstateDB.put(STR_seq(pbo),
                 pbo.toBuilder()
                      .setFromBcuid(network.root().bcuid)
                      .setState(PBFTStage.INIT)
                      .setLastUpdateTime(System.currentTimeMillis() + Config.getRandSleepForBan())
                      .build())
              } else {
                log.debug("Reject for other Vote ")
                Daos.viewstateDB.put(STR_seq(pbo),
                 pbo.toBuilder()
                      .setFromBcuid(network.root().bcuid)
                      .setState(PBFTStage.REJECT).setRejectState(PBFTStage.REJECT)
                      .setLastUpdateTime(System.currentTimeMillis() )
                      .build())
              }
              PBFTStage.REJECT
            }
          case n: Converge =>
            log.warn("unknow ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState);
            PBFTStage.NOOP
          case _ =>
            PBFTStage.NOOP
        }
      //        }
    }
  }
  def updateNodeStage(pbo: PVBase, state: PBFTStage): PBFTStage = {
    val strkey = STR_seq(pbo);
    val newpbo = if (state != pbo.getState) pbo.toBuilder().setState(state).setLastUpdateTime(System.currentTimeMillis()).build()
    else
      pbo;
    
    log.debug("put node stage=" + Daos.enc.bytesToHexStr((strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV).getBytes)+",state="+newpbo.getState)
    Daos.viewstateDB.put((strkey + "." + pbo.getFromBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + newpbo.getStateValue), newpbo, (strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV));
    state
  }
//  def outputList(ovs: BytesHashMap[Array[Byte]]): Unit = {
//    ovs.map { x =>
//      val p = PVBase.newBuilder().mergeFrom(x._2);
//    }
//  }
  def voteNodeStages(pbo: PVBase)(implicit dm: Votable = null): VoteResult = {
    val strkey = STR_seq(pbo);
    val ovs = Daos.viewstateDB.listBySecondKey((strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV));
    
    log.debug("get node stage=" + Daos.enc.bytesToHexStr((strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV).getBytes))
    
    if (ovs != null && ovs.size > 0) {
      val reallist = ovs.filter { ov => ov._2.getStateValue == pbo.getStateValue };
      log.debug("get list:allsize=" + ovs.size + ",statesize=" + reallist.size + ",state=" + pbo.getState)
//      outputList(ovs.get)
      if (dm != null) { //Vote only pass half
        dm.voteList(network, pbo, reallist) 
      } else {
        Undecisible()
      }
    } else {
      Undecisible()
    }
  }
 
  val VIEW_ID_PROP = "org.bc.pbft.view.state"
}