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

@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor],classOf[CMDService]
) )
class PZPRouteMessage extends PSMPZP[PSRouteMessage] {
  override def service = PZPRouteMessageService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPRouteMessageService extends OLog with PBUtils with LService[PSRouteMessage] with PMNodeHelper with LogHelper
    with NodeSetHelper with BitMap {

  var cdl = new CountDownLatch(0)

  override def onPBPacket(pack: FramePacket, pbo: PSRouteMessage, handler: CompleteHandler) = {
    var ret = PRetRouteMessage.newBuilder();

    val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {

      MDCSetBCUID(network)

      try {
        val net = networkByID(pbo.getNid);
        val nexthops = pb2scala(pbo.getNextHops);
        val start = System.currentTimeMillis();
        MDCSetMessageID("V|" + pbo.getMessageid);

        val strencbits = net.node_strBits();
        
        val bodybb = Right(pbo.getBody)
        
        log.debug("nexthops=" + nexthops);
        if (strencbits.equals(pbo.getEncbits) && bodybb != null) {
          net.wallMessage(pbo.getGcmd, bodybb , pbo.getMessageid)(nexthops)
        } else {
          log.warn("bit end not equals message gcmd=:" + pbo.getGcmd + ",netenc=" + strencbits
            + ",pboenc=" + pbo.getEncbits + ",body=");
          val bits = mapToBigInt(pbo.getEncbits)
          net.bwallMessage(pbo.getGcmd,  bodybb, bits)
          //                BitMap.hexToMapping(pbo.get))
        }

        //      net.wallMessage(pbo.getGcmd, pbo.getBody, messageid);
        ret.setPendingCount(net.pendingNodes.size)
        ret.setDnodeCount(net.directNodes.size)
        ret.setBitencs(net.node_strBits)
        ret.setRetMessage("TotalCost:" + (System.currentTimeMillis() - start))

        //      }
      } catch {
        case fe: NodeInfoDuplicated => {
          ret.clear();
          ret.setRetCode(-1).setRetMessage("" + fe.getMessage)
        }
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage("" + e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage("" + t.getMessage)
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
  override def cmd: String = PCommand.RRR.name();
}
