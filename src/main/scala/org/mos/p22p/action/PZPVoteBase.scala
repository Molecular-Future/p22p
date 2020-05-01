package org.mos.p22p.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.p22p.exception.NodeInfoDuplicated;
import org.mos.p22p.exception.FBSException
import org.mos.p22p.PSMPZP
import org.mos.p22p.model.P22P.PBFTStage
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PRetJoin
import org.mos.p22p.model.P22P.PVBase
import org.mos.p22p.model.P22P.PVType
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

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPVoteBase extends PSMPZP[PVBase] {
  override def service = PZPVoteBaseService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPVoteBaseService extends LogHelper with PBUtils with LService[PVBase] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PVBase, handler: CompleteHandler) = {
    MDCSetMessageID(pbo.getMTypeValue + "|" + pbo.getMessageUid)

    var ret = PRetJoin.newBuilder();
    try {
      val network = networkByID(pbo.getNid)
      if (network == null) {
        ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      } else {
        MDCSetBCUID(network)
        log.debug("VoteBase:MType=" + pbo.getMType + ":State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",SN=" + pbo.getStoreNum + ",VC=" + pbo.getViewCounter + ",O=" + pbo.getOriginBcuid + ",F=" + pbo.getFromBcuid
          + ",Reject=" + pbo.getRejectState + ",from=" + pbo.getFromBcuid)

        network.checkingHealthy.lastNodesCheckTime.put(pbo.getFromBcuid, System.currentTimeMillis());
        
        pbo.getMType match {
          case PVType.NETWORK_IDX | PVType.VIEW_CHANGE | PVType.STR_PBFT_VOTE =>
            network.voteQueue.appendInQ(pbo)
          case _ =>
            log.debug("unknow vote message:type=" + pbo.getMType)
        }
        if (pbo.getState == PBFTStage.UNRECOGNIZED) {
          log.debug("unknow current state:" + pbo.getState);
        }

      }

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
      try {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      } finally {
        MDCRemoveMessageID
      }

    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.VOT.name();
}
