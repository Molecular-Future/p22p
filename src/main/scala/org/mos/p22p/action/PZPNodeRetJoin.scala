package org.mos.p22p.action

import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.p22p.exception.NodeInfoDuplicated;
import org.mos.p22p.exception.FBSException
import org.mos.p22p.PSMPZP
import org.mos.p22p.core.MessageSender
import org.mos.p22p.node.Networks
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PRetJoin
import org.mos.p22p.model.P22P.PSJoin
import org.mos.p22p.utils.Config
import org.mos.p22p.utils.LogHelper

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.outils.serialize.SessionIDGenerator
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import java.util.Random
import org.mos.p22p.Daos
import java.util.concurrent.ConcurrentLinkedQueue
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import com.google.common.cache.Cache
import org.mos.mcore.tools.time.JodaTimeHelper

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPNodeRetJoin extends PSMPZP[PRetJoin] {
  override def service = PZPNodeRetJoinService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPNodeRetJoinService extends LogHelper with PBUtils with LService[PRetJoin] with PMNodeHelper {

  override def onPBPacket(pack: FramePacket, pbo: PRetJoin, handler: CompleteHandler) = {
    //    log.debug("JoinService::" + pack.getFrom() + ",OP=" + pbo.getOp)
    var ret = PRetJoin.newBuilder();
    val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {

      try {
        if (StringUtils.equals(pbo.getMessageid, network.joinNetwork.joinMessageId)) {
//            network.joinNetwork.recvJoinBuff.offer();
          val from = fromPMNode(pbo.getMyInfo);
          network.joinNetwork.recvJoinBuff.offer((from,pack));
        }
      } catch {
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage("" + t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.RJI.name();
}
