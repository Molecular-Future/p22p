package org.mos.p22p.action

import java.util.concurrent.CountDownLatch

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.mcore.crypto.BitMap
import org.mos.p22p.PSMPZP
import org.mos.p22p.exception.FBSException
import org.mos.p22p.exception.NodeInfoDuplicated
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PRetRouteMessage
import org.mos.p22p.model.P22P.PSRouteMessage
import org.mos.p22p.utils.LogHelper
import org.mos.p22p.utils.NodeSetHelper

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.mos.p22p.utils.BCPacket
import org.mos.p22p.model.P22P.MRSPacket
import org.mos.p22p.node.Networks
import onight.tfw.outils.serialize.UUIDGenerator
import org.apache.commons.lang3.StringUtils
import org.mos.p22p.utils.PacketIMHelper._
import scala.collection.JavaConverters._


@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPMRSMessage extends PSMPZP[MRSPacket] {
  override def service = PZPMRSMessageService
  
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPMRSMessageService extends OLog with PBUtils with LService[MRSPacket] with PMNodeHelper with LogHelper
  with NodeSetHelper with BitMap {

  var cdl = new CountDownLatch(0)

  override def onPBPacket(pack: FramePacket, pbo: MRSPacket, handler: CompleteHandler) = {
    var ret = MRSPacket.newBuilder();
    val network = networkByID(pbo.getNid)
    if (network == null) {
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {

        val originPack = BCPacket.extractWrapPacket(pbo);
        if (pbo.getLocId.equals(network.loc_id)) {
          val messageId = if(StringUtils.isBlank(pbo.getOriginMessageid)){
            UUIDGenerator.generate()
          }else{
            pbo.getOriginMessageid
          }
          for (i <- 0 to pbo.getBcUidsCount - 1) {
            val bcuid = pbo.getBcUids(i);
            log.debug("route message to bcuid="+bcuid+",gcmd="+originPack.getCMD+originPack.getModule+",messageId="+messageId+",uri="+pbo.getUris(i)
                +",origin_"+pbo.getOriginBcuid);
            network.postMessageWithOrigin(originPack.getCMD+originPack.getModule, Right(pbo.getBody), messageId, bcuid, originPack.getFixHead.getPrio,pbo.getOriginBcuid);
          }
          network.checkingHealthy.lastNodesCheckTime.put(pbo.getOriginBcuid, System.currentTimeMillis());

          //          (originPack.getGlobalCMD, Right(pbo.getBody), "", originPack.getFixHead.getPrio)
        } else {
          log.warn(s"drop not same loc_id:${network.loc_id},packloc_id=${pbo.getLocId}, from = "+ pack.getFrom()+", origin_bcuid="+pbo.getOriginBcuid
             +",dstbcuids=["+pbo.getBcUidsList.asScala.foldLeft("")((a,b)=>a+","+b)+"]")
        }
        //      }
      } catch {
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
        }
      } finally {
        try {
          handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
        } finally {
          MDCRemoveMessageID
        }
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.MRS.name();
}
