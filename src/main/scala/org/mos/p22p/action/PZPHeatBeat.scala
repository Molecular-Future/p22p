package org.mos.p22p.action

import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.mos.p22p.exception.NodeInfoDuplicated;
import org.mos.p22p.exception.FBSException
import org.mos.p22p.PSMPZP
import org.mos.p22p.model.P22P.PCommand
import org.mos.p22p.model.P22P.PMNodeInfo
import org.mos.p22p.model.P22P.PRetNodeInfo
import org.mos.p22p.model.P22P.PSNodeInfo
import org.mos.p22p.utils.LogHelper

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.proxy.IActor
import onight.tfw.ntrans.api.ActorService
import org.mos.p22p.utils.PacketIMHelper._


@NActorProvider
@Slf4j
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PZPHeatBeat extends PSMPZP[PSNodeInfo] {
  override def service = PZPHeatBeatService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPHeatBeatService extends OLog with PBUtils with LService[PSNodeInfo] with PMNodeHelper with LogHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeInfo, handler: CompleteHandler) = {
    //    log.debug("onPBPacket::" + pbo)
    var ret = PRetNodeInfo.newBuilder();
    val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
//        log.debug("get HBT from:" + pack.getFrom() + ":sent=" + pack.getExtStrProp("T__LOG_SENT"))
        //       pbo.getMyInfo.getNodeName
        ret.setCurrent(toPMNode(network.root))
        ret.setRetCode(0)
        val pending = network.pendingNodes;
        val directNodes = network.directNodes;
        network.onlineMap.put(pbo.getNode.getBcuid, fromPMNode(pbo.getNode))

        log.debug("pending=" + network.pendingNodes.size + "::" + network.pendingNodes)
        //      ret.addNodes(toPMNode(NodeInstance.curnode));
        pending.map { _pn =>
          log.debug("pending==" + _pn)
          if (StringUtils.equals(_pn.bcuid, pbo.getNode.getBcuid) &&
            !StringUtils.equals(_pn.name, pbo.getNode.getNodeName)) {
            network.changePendingNode(_pn.changeName(pbo.getNode.getNodeName));
          }
          ret.addPnodes(toPMNode(_pn));
        }
        directNodes.map { _pn =>
          log.debug("directnodes==" + _pn)
          if (StringUtils.equals(_pn.bcuid, pbo.getNode.getBcuid) &&(
            !StringUtils.equals(_pn.name, pbo.getNode.getNodeName)
            || !StringUtils.equals(_pn.v_address, pbo.getNode.getCoAddress))) {
            network.changeDirectNode(_pn.changeName(pbo.getNode.getNodeName).changeVaddr(pbo.getNode.getCoAddress));
          }
          ret.addDnodes(toPMNode(_pn));
        }
        ret.setBitEncs(network.node_strBits);
        log.info("response HBT to:" + pack.getFrom() + ":sent=" + pack.getExtStrProp("T__LOG_SENT"))
      } catch {
        case fe: NodeInfoDuplicated => {
          log.error("NodeInfoDuplicated")
          ret.clear();
          log.error("error:in heatbeat: NodeInfoDuplicated", fe);
          ret.setCurrent(toPMNode(network.root))
          ret.setRetCode(-1).setRetMessage(fe.getMessage + "")
        }
        case e: FBSException => {
          ret.clear()
          log.error("error:in heatbeat : FBSException", e);
          ret.setCurrent(toPMNode(network.root))
          ret.setRetCode(-2).setRetMessage(e.getMessage + "")
        }
        case t: Throwable => {
          log.error("error:in heatbeat", t);
          ret.clear()
          ret.setCurrent(toPMNode(network.root))
          ret.setRetCode(-3).setRetMessage("UNKNOWN")
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.HBT.name();
}
