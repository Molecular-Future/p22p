package org.mos.p22p.action

import com.google.protobuf.{ByteString, MessageOrBuilder}
import onight.tfw.outils.serialize.SerializerFactory
import org.apache.commons.codec.binary.Base64

trait PMNodeHelper {

  def networkByID(nid: String): Network = {
    Networks.networkByID(nid);
  }

  def toPMNode(n: Node): PMNodeInfo.Builder = {
    PMNodeInfo.newBuilder()
      .setUri(n.uri)
      .setCoAddress(n.v_address)
      .setNodeName(n.name)
      .setNodeIdx(n.node_idx)
      .setSign(n.sign)
      .setPubKey(n.pub_key).setStartupTime(n.startup_time).setTryNodeIdx(n.try_node_idx).setBcuid(n.bcuid)
      .setSendCc(n.counter.send.get).setRecvCc(n.counter.recv.get).setBlockCc(n.counter.blocks.get)
      .setLocId(n.loc_id).setLocGwuris(n.loc_gwuris)
  }

  def toFullPMNode(n: Node): PMNodeInfo.Builder = {
    val pm=PMNodeInfo.newBuilder().setUri(n.uri).setNodeName(n.name)
      .setPubKey(n.pub_key).setStartupTime(n.startup_time).setTryNodeIdx(n.try_node_idx).setBcuid(n.bcuid)
      .setPriKey(n.pri_key).setNodeIdx(n.node_idx)
      .setSendCc(n.counter.send.get).setRecvCc(n.counter.recv.get).setBlockCc(n.counter.blocks.get)
      .setLocId(n.loc_id).setLocGwuris(n.loc_gwuris)
      .setCoAddress(n.v_address)
    if(n.sign!=null){
      pm.setSign(n.sign)
    }
    pm;
  }
  val pser = SerializerFactory.getSerializer(SerializerFactory.SERIALIZER_PROTOBUF)

  def serialize(n: Node): String = {
    Base64.encodeBase64String(toBytes(toFullPMNode(n)))
  }

  def deserialize(str: String,_uri:String = null): PNode = {
    fromPMNode(pser.deserialize(Base64.decodeBase64(str), classOf[PMNodeInfo]),_uri)
  }

  def fromPMNode(pm: PMNodeInfoOrBuilder, _overrided_uri: String = null): PNode = {
    PNode(
      _name = pm.getNodeName, _node_idx = pm.getNodeIdx, //node info
      _sign = pm.getSign,
      _uri =
        if (_overrided_uri == null) pm.getUri else _overrided_uri, //
      _startup_time = pm.getStartupTime, //
      _pub_key = pm.getPubKey, //
      _counter = new CCSet(pm.getRecvCc, pm.getSendCc, pm.getBlockCc),
      _try_node_idx = pm.getTryNodeIdx,
      _bcuid = pm.getBcuid,
      _pri_key = pm.getPriKey,
      _v_address = pm.getCoAddress,
      _loc_id = pm.getLocId,
      _loc_gwuris = pm.getLocGwuris
    )
  }

  def toBytes(body: MessageOrBuilder): Array[Byte] = {
    pser.serialize(body).asInstanceOf[Array[Byte]]
  }
  def toByteString(body: MessageOrBuilder): ByteString = {
    ByteString.copyFrom(toBytes(body))
  }  

  def fromByteString[T](str: ByteString, clazz: Class[T]): T = {
    pser.deserialize(str.toByteArray(), clazz)
  }

}

