package org.mos.p22p.node.router

import com.google.protobuf.{ByteString, Message}
import onight.oapi.scala.traits.OLog

trait MessageRouter extends OLog {

  def broadcastMessage(gcmd:String,body: Either[Message,ByteString], from: Node,priority:Byte)(implicit to: Node,
    nextHops: IntNode = FullNodeSet(),
    network: Network,messageid:String): Unit = {
//        log.debug("broadcastMessage:cur=@" + to.node_idx + ",from.idx=" + from.node_idx + ",netxt=" + nextHops)
    to.counter.recv.incrementAndGet();
    //    from.counter.send.incrementAndGet();
//    network.updateConnect(from.node_idx,to.node_idx)
     
    to.processMessage(gcmd,body)
    nextHops match {
      case f: FullNodeSet =>
//        from.counter.send.incrementAndGet();
        routeMessage(gcmd,body,priority)(to, FlatSet(from.node_idx, network.node_bits), network,messageid)
      case none: EmptySet =>
        log.debug("Leaf Node");
      case subset: IntNode =>
//        from.counter.send.incrementAndGet();
        routeMessage(gcmd,body,priority)(to, subset, network,messageid)
    }

  }

  def routeMessage(gcmd:String,body: Either[Message,ByteString],priority:Byte)(implicit from: Node,
    nextHops: IntNode = FullNodeSet(), 
    network: Network,messageid:String)
}




