package org.mos.p22p.action

import scala.collection.JavaConversions._

import org.apache.commons.codec.binary.Base64
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.p22p.exception.FBSException
import org.mos.p22p.Daos
import org.mos.p22p.PSMPZP
import org.mos.p22p.model.P22P.NodeStateInfo
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PRetVoteState
import org.mos.p22p.model.P22P.PSVoteState
import org.mos.p22p.model.P22P.PVBase
import org.mos.p22p.utils.LogHelper

import com.google.protobuf.ByteString

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
@Provides(specifications = Array(classOf[ActorService], classOf[IActor],classOf[CMDService]
) )
class PZPStateInfo extends PSMPZP[PSVoteState] {
  override def service = PZPStateInfoService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPStateInfoService extends LogHelper with PBUtils with LService[PSVoteState] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSVoteState, handler: CompleteHandler) = {
    //    log.debug("onPBPacket::" + pbo)
    var ret = PRetVoteState.newBuilder();
    val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      MDCSetBCUID(network)

      val ss = network.stateStorage;
      try {
        //       pbo.getMyInfo.getNodeName
        val strkey = pbo.getV match {
          case v if v > 0 => ss.STR_seq(pbo.getTValue) + ".F." + v
          case _ => ss.STR_seq(pbo.getTValue)
        }

        Daos.viewstateDB.get(strkey) match {
          case ov if ov != null =>
            val pb = ov.toBuilder()
            pb.setContents(ByteString.copyFrom(Base64.encodeBase64(pb.getContents.toByteArray())))
            ret.setCur(NodeStateInfo.newBuilder().setV(pb).setK(strkey));
            val v = pbo.getV match {
              case v if v > 0 => v
              case _ => pb.getV
            }
            log.debug("view state:V=" + v);
            Daos.viewstateDB.listBySecondKey((ss.STR_seq(pbo.getTValue) + "." + pb.getOriginBcuid + "." + pb.getMessageUid + "." + v)) match {
              case ovs if ovs != null =>
                ovs.map { x =>
                  //                ret.setNodes(x$1)
                  val ppb = x._2.toBuilder();
                  ppb.setContents(ByteString.copyFrom(Base64.encodeBase64(ppb.getContents.toByteArray())))
                  ret.addNodes(NodeStateInfo.newBuilder().setV(ppb).setK(new String(x._1)))
                }
            }
          case _ =>
            ret.setRetCode(-1).setRetMessage("NOT FOUND CURR")
        }
      } catch {
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
  override def cmd: String = PCommand.VTI.name();
}
