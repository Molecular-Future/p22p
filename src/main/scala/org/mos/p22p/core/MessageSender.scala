package org.mos.p22p.core

import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.Provides
import org.mos.mcore.api.ICryptoHandler
import org.mos.p22p.node.Network
import org.mos.p22p.node.Networks
import org.mos.p22p.node.Node
import org.mos.p22p.utils.BCPacket
import org.mos.p22p.utils.Config
import org.mos.p22p.utils.PacketIMHelper._

import com.google.protobuf.ByteString
import com.google.protobuf.Message

import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CallBack
import onight.tfw.ntrans.api.annotation.ActorRequire
import onight.tfw.otransio.api.IPacketSender
import onight.tfw.otransio.api.NonePackSender
import onight.tfw.otransio.api.PSender
import onight.tfw.otransio.api.PackHeader
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.ntrans.api.ActorService
import onight.tfw.otransio.api.PSenderService
import onight.tfw.ntrans.api.NActor
import java.util.HashMap
import java.util.ArrayList
import scala.collection.mutable.Buffer
import onight.tfw.outils.serialize.UUIDGenerator
import org.mos.p22p.utils.PacketIMHelper

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[PSenderService]))
class CMessageSender extends NActor {

  //http. socket . or.  mq  are ok
  @PSender
  var sockSender: IPacketSender = new NonePackSender();

  def setSockSender(send: IPacketSender): Unit = {
    sockSender = send;
    MessageSender.sockSender = sockSender;
  }
  def getSockSender(): IPacketSender = {
    sockSender
  }
  @ActorRequire(name = "bc_crypto", scope = "global")
  var encApi: ICryptoHandler = null;
  def setEncApi(ecapi: ICryptoHandler): Unit = {
    encApi = ecapi;
    MessageSender.encApi = ecapi;
  }
  def getEncApi(): ICryptoHandler = {
    encApi
  }
}

object MessageSender extends OLog {
  var sockSender: IPacketSender = new NonePackSender();

  val PACK_SIGN = "_s";
  var encApi: ICryptoHandler = null;

  def appendUid(pack: BCPacket, node: Node, origin_bcuid: String = null)(implicit network: Network): Unit = {
    if (network.isLocalNode(node)) {
      pack.getExtHead.remove(PackHeader.PACK_TO);
    } else {
      pack.putHeader(PackHeader.PACK_TO, node.bcuid);
      pack.putHeader(PackHeader.PACK_URI, node.uri);
    }

    if (Config.MESSAGE_SIGN == 1 && !pack.isBodySigned()) {
      val bb = pack.genBodyBytes()
      val shabb = encApi.sha256(bb);
      val signm = encApi.sign(encApi.hexStrToBytes(network.root().pri_key), shabb)
      pack.putHeader(PACK_SIGN, encApi.bytesToHexStr(signm));
      pack.setBodySigned(true);
    }
    pack.putHeader(PackHeader.PACK_FROM, network.root().bcuid);

    if (StringUtils.isNotBlank(origin_bcuid)) {
      pack.putHeader(PacketIMHelper.PACK_ORIGIN_FORM, origin_bcuid);
    }
  }

  def zipPacket(gcmd: String, body: Either[Message, ByteString], nodes: Iterable[Node], messageId: String, priority: Byte = 0)(implicit network: Network): List[BCPacket] = {
    val groups = new HashMap[String, ArrayList[(String, String)]]()
    val retpacks = Buffer.empty[BCPacket];
    nodes.map(node => {
      var loc_id = node.loc_id;
      if (!StringUtils.equals(network.loc_id, loc_id) && network.nodesByLocID.contains(loc_id)) {
        var existList = groups.get(node.loc_id);
        if (existList == null) {
          existList = new ArrayList[(String, String)]();
          groups.put(node.loc_id, existList);
        }
        existList.add((node.bcuid, node.uri))
      } else {
        val pack = body match {
          case Left(m)  => BCPacket.buildAsyncFrom(m, gcmd.substring(0, 3), gcmd.substring(3));
          case Right(b) => BCPacket.buildAsyncFrom(b.toByteArray(), gcmd.substring(0, 3), gcmd.substring(3));
        }
        if (priority > 0) {
          pack.getFixHead.setPrio(priority)
        }
        appendUid(pack, node)
        sockSender.post(pack)
      }
    })
    groups.forEach((loc_id, list) => {
      val bcuids = new ArrayList[String]();
      val uris = new ArrayList[String]();
      list.forEach(v => {
        bcuids.add(v._1)
        uris.add(v._2)
      })

      val gwnode = network.nodesByLocID.get(loc_id) match {
        case Some((counter, vlist)) =>
          if (vlist != null && vlist.size() > 0) {
            if (counter.get > 100000000 || counter.get < 0) {
              counter.set(0);
            }
            var tn = vlist.get(Math.abs(counter.incrementAndGet()) % vlist.size());
            var cc = 0;
            while (!network.directNodeByBcuid.contains(tn.bcuid) && cc < vlist.size()) {
              tn = vlist.get(Math.abs(counter.incrementAndGet()) % vlist.size());
              cc = cc + 1;
            }
            tn
          } else {
            null
          }
        case _ =>
          null
      }
      if (gwnode != null) {
        val pack = body match {
          case Left(m)  => BCPacket.buildAsyncFrom(m, gcmd.substring(0, 3), gcmd.substring(3));
          case Right(b) => BCPacket.buildAsyncFrom(b.toByteArray(), gcmd.substring(0, 3), gcmd.substring(3));
        }
        if (priority > 0) {
          pack.getFixHead.setPrio(priority)
        }
        val wp = BCPacket.wrapPacket(pack, loc_id, bcuids, uris, network.netid, network.root().bcuid, messageId)
        wp.putHeader(PackHeader.PACK_URI, gwnode.uri);
        wp.putHeader(PackHeader.PACK_TO, gwnode.bcuid);
        if (Config.MESSAGE_SIGN == 1 && !wp.isBodySigned()) {
          val bb = pack.genBodyBytes()
          val shabb = encApi.sha256(bb);
          val signm = encApi.sign(encApi.hexStrToBytes(network.root().pri_key), shabb)
          wp.putHeader(PACK_SIGN, encApi.bytesToHexStr(signm));
          wp.setBodySigned(true);
        }
        wp.putHeader(PackHeader.PACK_FROM, network.root().bcuid);

        retpacks.append(wp);
      } else {
        log.error("cannot route packet for locid=" + loc_id);
      }
    })
    retpacks.toList
  }

  def verifyMessage(pack: FramePacket): Option[Boolean] = {
    val fromuid = pack.getExtStrProp(PackHeader.PACK_FROM)
    if (StringUtils.isNotBlank(fromuid)) {
      val net = if (fromuid.startsWith("D")) {
        Networks.networkByID("dpos")
      } else if (fromuid.startsWith("R")) {
        Networks.networkByID("raft")
      } else {
        null
      }
      if (net != null) {
        val node = net.nodeByBcuid(fromuid)
        pack.getExtStrProp(PACK_SIGN) match {
          case n if StringUtils.isNotBlank(n) && n.length() >= 128 =>
            // TODO: maybe wrong..
            val bb = pack.getBody
            val pubkey = n.substring(0, 128);
            if (node == net.noneNode || StringUtils.equalsIgnoreCase(pubkey, node.pub_key)) {
              val shabb = encApi.sha256(bb)
              val result = encApi.verify(encApi.hexStrToBytes(pubkey), shabb, encApi.hexStrToBytes(n))
              Some(result);
            } else {
              log.debug(fromuid + "messageverify error:fatal:" + pack.getExtHead + ",sign=" +
                pack.getExtStrProp(MessageSender.PACK_SIGN) + ",pubkey not equal:" +
                pubkey + ",nodepubkey=" + node.pub_key)
              Some(false);
            }
          case _ =>
            None
        }
      } else {
        None
      }
    } else {
      None
    }

  }

  def sendMessage(gcmd: String, body: Message, node: Node, cb: CallBack[FramePacket], priority: Byte = 0, timeoutms: Long = Config.TIMEOUT_MS_MESSAGE)(implicit network: Network) {
    val pack = BCPacket.buildSyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }

    appendUid(pack, node)
    log.trace("sendMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    try {
      cb.onSuccess(sockSender.send(pack, timeoutms))
    } catch {
      case e: Exception =>
        log.warn("sendMessageFailed:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo(), e)
        cb.onFailed(e, pack);
    }

  }

  def asendMessage(gcmd: String, body: Message, node: Node, cb: CallBack[FramePacket], priority: Byte = 0)(implicit network: Network) {
    val pack = BCPacket.buildSyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }

    appendUid(pack, node)
    //    log.trace("sendMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.asyncSend(pack, cb)
  }

  //  def wallMessageToPending(gcmd: String, body: Message, priority: Byte = 0)(implicit network: Network) {
  //    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
  //    if (priority > 0) {
  //      pack.getFixHead.setPrio(priority)
  //    }
  //    log.trace("wallMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
  //    network.pendingNodes.map { node =>
  //      appendUid(pack, node)
  //      sockSender.post(pack)
  //    }
  //  }

  def wallMessageToPending(gcmd: String, body: ByteString, messageId: String, priority: Byte = 0)(implicit network: Network) {
    //    val pack = BCPacket.buildAsyncFrom(body.toByteArray(), gcmd.substring(0, 3), gcmd.substring(3));
    //    if (priority > 0) {
    //      pack.getFixHead.setPrio(priority)
    //    }
    val nodes = network.pendingNodes.toList
    zipPacket(gcmd, Right(body), nodes, messageId, priority).foreach(zp => {
      sockSender.post(zp)
    })
    //    log.trace("wallMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    //    network.pendingNodes.map { node =>
    //      appendUid(pack, node)
    //      sockSender.post(pack)
    //    }
  }

  def postMessage(gcmd: String, body: Either[Message, ByteString], node: Node, priority: Byte = 0, origin_bcuid: String = null)(implicit network: Network): Unit = {
    //    if("TTTPZP".equals(gcmd)){
    //      return;
    //    }
    val pack = body match {
      case Left(m)  => BCPacket.buildAsyncFrom(m, gcmd.substring(0, 3), gcmd.substring(3));
      case Right(b) => BCPacket.buildAsyncFrom(b.toByteArray(), gcmd.substring(0, 3), gcmd.substring(3));
    }
    if (priority > 0) {
      pack.getFixHead.setPrio(priority)
    }
    appendUid(pack, node, origin_bcuid)
    //    log.trace("postMessage:" + pack)
    //    log.trace("postMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.post(pack)
  }

  def wallMessage(gcmd: String, body: Either[Message, ByteString], nodes: Iterable[Node], messageId: String, priority: Byte = 0)(implicit network: Network): Unit = {

    zipPacket(gcmd, body, nodes, messageId, priority).foreach(zp => {
      sockSender.post(zp)
    })
  }

  def replyPostMessage(gcmd: String, node: Node, body: Message)(implicit network: Network) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, node); //frompack.getExtStrProp(PackHeader.PACK_FROM));
    sockSender.post(pack)
  }

  def dropNode(node: Node) {
    sockSender.tryDropConnection(node.bcuid);
  }
  def dropNode(bcuid: String) {
    sockSender.tryDropConnection(bcuid);
  }

  def changeNodeName(oldName: String, newName: String) {
    sockSender.changeNodeName(oldName, newName);
  }

  def setDestURI(bcuid: String, uri: String) {
    sockSender.setDestURI(bcuid, uri);
  }
}

