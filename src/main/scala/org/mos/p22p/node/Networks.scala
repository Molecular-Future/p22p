package org.mos.p22p.node

import java.net.URL
import java.util.{ArrayList, HashMap}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.{ByteString, Message}
import onight.oapi.scala.traits.OLog
import onight.tfw.async.CallBack
import onight.tfw.ntrans.api.NActor
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.outils.serialize.UUIDGenerator
import org.apache.commons.lang3.StringUtils
import org.mos.mcore.crypto.BitMap

import scala.collection.Iterable
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

case class BitEnc(bits: BigInt) extends BitMap {
  val strEnc: String = hexToMapping(bits);
}
case class Network(netid: String, nodelist: String,loc_id:String) extends OLog with LocalNode //
{
  val directNodeByBcuid: Map[String, Node] = Map.empty[String, Node];
  val directNodeByIdx: Map[Int, Node] = Map.empty[Int, Node];
  val pendingNodeByBcuid: Map[String, Node] = Map.empty[String, Node];
  val connectedMap: Map[Int, Map[Int, Int]] = Map.empty[Int, Map[Int, Int]];
  val onlineMap: Map[String, Node] = new ConcurrentHashMap[String, Node];

  val nodesByLocID: Map[String, (AtomicInteger, ArrayList[Node])] = Map.empty;

  //  var _node_bits = BigInt(0)
  var bitenc = BitEnc(BigInt(0))

  def node_bits(): BigInt = bitenc.bits
  def node_strBits(): String = bitenc.strEnc;

  def resetNodeBits(_new: BigInt): Unit = {
    this.synchronized {
      bitenc = BitEnc(_new)
      circleNR = CircleNR(_new)
    }
  }

  var circleNR: CircleNR = CircleNR(node_bits())

  val noneNode = PNode(_name = "NONE", _node_idx = 0, "",_bcuid="",
    _try_node_idx = 0)

  // def nodeByBcuid(name: String): Node = directNodeByBcuid.getOrElse(name, noneNode);
  /**如果当前DN中查询不到, 到PN中查询, 防止突然停节点, 查询不到节点信息*/
  def nodeByBcuid(name: String): Node = directNodeByBcuid.getOrElse(name, pendingNodeByBcuid.getOrElse(name, noneNode));
  def nodeByIdx(idx: Int) = directNodeByIdx.get(idx);

  def directNodes: Iterable[Node] = directNodeByBcuid.values

  def pendingNodes: Iterable[Node] = pendingNodeByBcuid.values

  def addDNode(pnode: Node): Option[Node] = {

    val node = pnode.changeIdx(pnode.try_node_idx)
    this.synchronized {
      if (!directNodeByBcuid.contains(node.bcuid) && node.node_idx >= 0 && !node_bits().testBit(node.node_idx)) {
        if (StringUtils.equals(node.bcuid, root().bcuid)) {
          resetRoot(node)
        }
        directNodeByBcuid.put(node.bcuid, node)
        resetNodeBits(node_bits.setBit(node.node_idx));

        if (StringUtils.isNotBlank(node.uri)) {
          MessageSender.setDestURI(node.bcuid, node.uri);
        }

        directNodeByIdx.put(node.node_idx, node);
        if (node.loc_gwuris.contains(node.uri)) {
          nodesByLocID.get(node.loc_id) match {
            case Some((counter, nodes)) =>
              var found = false;
              nodes.forEach(n => if (node.bcuid.equals(n.bcuid)) { found = true })
              if (!found) {
                nodes.add(node);
              }
            case None =>
              val nodes = new ArrayList[Node]
              nodes.add(node);
              nodesByLocID.put(node.loc_id, (new AtomicInteger(0), nodes))
          }
        }

        removePendingNode(node)
        Some(node)
      } else {
        None
      }
    }
  }

  def inNetwork(): Boolean = {
    // log.info("root().node_idx > 0=" + (root().node_idx > 0) + " , nodeByIdx(root().node_idx)=" + nodeByIdx(root().node_idx))
    root().node_idx > 0 && nodeByIdx(root().node_idx) != None;
  }

  def removeDNode(node: Node): Option[Node] = {
    if (directNodeByBcuid.contains(node.bcuid)) {
      resetNodeBits(node_bits.clearBit(node.node_idx));

      if (node.loc_gwuris.contains(node.uri)) {
        nodesByLocID.get(node.loc_id) match {
          case Some((counter, nodes)) =>
            var foundidx: Int = -1;
            for (i <- 0 to nodes.size() - 1 if foundidx == -1) {
              val checknode = nodes.get(i);
              if (node.bcuid.equals(checknode.bcuid)) { foundidx = i; }
            }
            if (foundidx >= 0) {
              nodes.remove(foundidx)
            }
          case None =>
        }
      }

      directNodeByBcuid.remove(node.bcuid)
      directNodeByIdx.remove(node.node_idx);

    } else {
      None
    }
  }
  //  var node_idx = _node_idx; //全网确定之后的节点id
  def changePendingNode(node: Node): Boolean = {
    this.synchronized {
      if (pendingNodeByBcuid.contains(node.bcuid)) {
        if (node.try_node_idx == 0 && node.node_idx == 0) {
          log.error("update pending zero error" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
          false
        } else {
          pendingNodeByBcuid.put(node.bcuid, node);
          log.debug("update pending:" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
          true
        }
      } else {
        false
      }
    }
  }
  def changeDirectNode(node: Node): Boolean = {
    this.synchronized {
      if (directNodeByBcuid.contains(node.bcuid)) {
        directNodeByBcuid.put(node.bcuid, node);
        log.debug("update directnode:" + directNodeByBcuid.size + ",p=" + node.bcuid)
        true
      } else {
        false
      }
    }
  }
  def addPendingNode(node: Node): Boolean = {
    if (node.try_node_idx == 0 && node.node_idx == 0) {
      log.error("pending node zero id error,bcuid=" + node.bcuid);
      false
    } else {
      this.synchronized {
        if (directNodeByBcuid.contains(node.bcuid)) {
          // log.debug("directNode exists in DirectNode bcuid=" + node.bcuid);
          false
          //      } else if (pendingNodeByBcuid.contains(node.bcuid)) {
          //        log.debug("pendingNode exists PendingNodes bcuid=" + node.bcuid);
          //        false
        } else {
          val sameuri = pendingNodeByBcuid.filter(p => {
            p._2.uri.equals(node.uri) && !p._2.bcuid.equals(node.bcuid)
          })
          if (sameuri.size > 0) {
            log.debug("sameuri:" + sameuri);
            false
          } else {
            pendingNodeByBcuid.put(node.bcuid, node);

            if (node.loc_gwuris.contains(node.uri)) {
              nodesByLocID.get(node.loc_id) match {
                case Some((counter, nodes)) =>
                  var found = false;
                  nodes.forEach(n => if (node.bcuid.equals(n.bcuid)) { found = true })
                  if (!found) {
                    nodes.add(node);
                  }
                case None =>
                  val nodes = new ArrayList[Node]
                  nodes.add(node);
                  nodesByLocID.put(node.loc_id, (new AtomicInteger(0), nodes))
              }
            }
            
            log.debug("addpending:" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
            true
          }
        }
      }
    }
  }
  def pending2DirectNode(nodes: List[Node]): Boolean = {
    this.synchronized {
      nodes.map { node =>
        //remove pendings.
        removePendingNode(node)

        pendingNodes.filter { pn =>
          pn.try_node_idx == node.node_idx || StringUtils.equals(pn.uri, node.uri)
        }.map { rmn =>
          removePendingNode(rmn)
        }
        addDNode(node)
      }.filter { _ == None }.size == 0
    }
  }

  def removePendingNode(node: Node): Boolean = {
    this.synchronized {
      if (!pendingNodeByBcuid.contains(node.bcuid)) {
        false
      } else {
        pendingNodeByBcuid.remove(node.bcuid);
        log.debug("remove:" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
        true
      }
    }
  }

  def updateConnect(fromIdx: Int, toIdx: Int) = {
    if (fromIdx != -1 && toIdx != -1)
      connectedMap.synchronized {
        connectedMap.get(fromIdx) match {
          case Some(m) =>
            m.put(toIdx, fromIdx)
          case None =>
            connectedMap.put(fromIdx, scala.collection.mutable.Map[Int, Int](toIdx -> fromIdx))
        }
        connectedMap.get(toIdx) match {
          case Some(m) =>
            m.put(fromIdx, toIdx)
          case None =>
            connectedMap.put(toIdx, scala.collection.mutable.Map[Int, Int](fromIdx -> toIdx))
        }
        //      log.debug("map="+connectedMap)
      }
  }

  def wallMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = "", priority: Byte = 0)(implicit nextHops: IntNode = FullNodeSet()): Unit = {
    //if (circleNR.encbits.bitCount > 0) {
    //      log.debug("wall to DCircle:" + messageId + ",Dnodescount=" + directNodes.size + ",enc=" +
    //        node_strBits())
    // circleNR.broadcastMessage(gcmd, body, from = root(),priority)(toN = root(), network = this, nextHops = nextHops, messageid = messageId)
    //}
    //pendingNodes.map(n =>
    //  {
    //    log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //    MessageSender.postMessage(gcmd, body, n,priority)(this)
    //  })
    dwallMessage(gcmd, body, messageId, priority);
  }

  def sendMessage(gcmd: String, body: Message, node: Node, cb: CallBack[FramePacket], priority: Byte = 0, timeoutms: Long = Config.TIMEOUT_MS_MESSAGE): Unit = {
    MessageSender.sendMessage(gcmd, body, node, cb, priority, timeoutms)(this)
  }
  def asendMessage(gcmd: String, body: Message, node: Node, cb: CallBack[FramePacket], priority: Byte = 0): Unit = {
    MessageSender.asendMessage(gcmd, body, node, cb, priority)(this)
  }
  def bwallMessage(gcmd: String, body: Either[Message, ByteString], bits: BigInt, messageId: String = "", priority: Byte = 0): Unit = {
    val nodes = directNodes.++:(pendingNodes).filter(n => if(n.node_idx>=0)bits.testBit(n.node_idx)else if(n.try_node_idx>=0)bits.testBit(n.try_node_idx)else false  )
    MessageSender.wallMessage(gcmd, body, nodes,messageId, priority)(this)

    //    directNodes.map(n =>
    //      if (bits.testBit(n.node_idx)) {
    //        log.debug("bitpost to direct:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //        MessageSender.postMessage(gcmd, body, n, priority)(this)
    //      })
    //    pendingNodes.map(n =>
    //      if (bits.testBit(n.try_node_idx)) {
    //        log.debug("bitpost to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //        MessageSender.postMessage(gcmd, body, n, priority)(this)
    //      })
  }

  def dwallMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = "",
                   priority: Byte = 0): Unit = {

    val nodes = directNodes.++:(pendingNodes)
    MessageSender.wallMessage(gcmd, body, nodes, messageId,priority)(this)

    //    directNodes.map(n =>
    //      {
    ////        log.debug("post to direct:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //        MessageSender.postMessage(gcmd, body, n, priority)(this)
    //      })
    //    pendingNodes.map(n =>
    //      {
    ////        log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //        MessageSender.postMessage(gcmd, body, n, priority)(this)
    //      })
  }
  def postMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = "", toN: String, priority: Byte = 0): Unit = {
    postMessageWithOrigin(gcmd,body,messageId,toN,priority,null);
  }
  def postMessageWithOrigin(gcmd: String, body: Either[Message, ByteString], messageId: String = "", toN: String, priority: Byte = 0,origin_bcuid:String=null): Unit = {
    directNodeByBcuid.get(toN) match {
      case Some(n) =>
        MessageSender.postMessage(gcmd, body, n, priority,origin_bcuid)(this)
      case None =>
        log.debug("cannot Found Result from direct nodes,try pending");
        pendingNodeByBcuid.get(toN) match {
          case Some(n) =>
            MessageSender.postMessage(gcmd, body, n, priority,origin_bcuid)(this)
          case None =>
            log.debug("cannot Found Result from pending nodes");
        }
    }

  }

  def wallOutsideMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = "", priority: Byte = 0): Unit = {

    val nodes = directNodes.++:(pendingNodes).filter(!isLocalNode(_))
    MessageSender.wallMessage(gcmd, body, nodes, messageId,priority)(this)
    //    directNodes.map { n =>
    //      if (!isLocalNode(n)) {
    ////        log.trace("post to directNode:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //        MessageSender.postMessage(gcmd, body, n, priority)(this)
    //      }
    //    }
    //    pendingNodes.map(n =>
    //      {
    //        if (!isLocalNode(n)) {
    ////          log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
    //          MessageSender.postMessage("VOTPZP", body, n, priority)(this)
    //        }
    //      })
  }
  //

  val joinNetwork = JoinNetwork(this, nodelist.split(",").map { x =>
    log.debug("x=" + x)
    PNode.fromURL(x, this.netid, Config.LOC_ID, Config.LOC_GWURIS);
  });
  val stateStorage = StateStorage(this);
  val voteQueue = VoteQueue(this);
  val checkingHealthy = CheckingHealthy(this);
  val voteNodeMap = VoteNodeMap(this, voteQueue);
  def startup(): Unit = {

    val uris = new ArrayList[Node]();
    Config.LOC_GWURIS.split(",").map(uri => {
      try {
        val eurl = new URL(uri)
        //       uris.add(eurl.toString())
        PNode.fromURL(eurl.toString(), this.netid, Config.LOC_ID, Config.LOC_GWURIS);
      } catch {
        case t: Throwable =>
          log.warn("error in parse uri:" + uri);
      }
    })
    nodesByLocID.put(Config.LOC_ID, (new AtomicInteger(0), uris));

    joinNetwork.init();
    Daos.ddc.scheduleWithFixedDelaySecond(joinNetwork, 5, 10)
    Daos.ddc.scheduleWithFixedDelaySecond(checkingHealthy, 10, Config.TICK_CHECK_HEALTHY)
    Daos.ddc.scheduleWithFixedDelaySecond(voteNodeMap, 10, Config.TICK_VOTE_MAP)
    Daos.ddc.scheduleWithFixedDelaySecond(VoteWorker(this, voteQueue), 10, Config.TICK_VOTE_WORKER)
  }
}

object Networks extends NActor with LogHelper {
  //  val raft: Network = new Network("raft","tcp://127.0.0.1:5100");
  val netsByID = new HashMap[String, Network]();

  val instanceid = "NET_" + UUIDGenerator.generate();

  def networkByID(netid: String): Network = {
    netsByID.get(netid);
  }

}



