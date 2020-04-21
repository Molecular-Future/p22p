package org.mos.p22p.node

import java.net.URL

import com.google.protobuf.{ByteString, Message}
import onight.oapi.scala.traits.OLog
import onight.tfw.mservice.NodeHelper
import onight.tfw.outils.conf.PropHelper
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringUtils

import scala.util.Either

sealed trait Node {
  def processMessage(gcmd: String, body: Either[Message, ByteString])(implicit network: Network): Unit
  def changeIdx(idx: Int): Node;
  def changeVaddr(vaddr: String): Node;
  def changeName(name: String): Node;
  def name: String
  def node_idx: Int;
  def bcuid: String;
  def pub_key: String;
  def pri_key: String;
  def v_address: String;
  def counter: CCSet;
  def startup_time: Long;
  def sign: String;
  def uri: String;
  def uris: Array[String];
  def try_node_idx: Int;
  def loc_id:String;//指定的机房ids
  def loc_gwuris:String;//机房的外网gateway uri
}

case class PNode(_name: String, _node_idx: Int, //node info
    _sign: String,
    _uri: String = "", //
    _startup_time: Long = System.currentTimeMillis(), //
    _pub_key: String = "", //
    _counter: CCSet = CCSet(),
    _try_node_idx: Int = 0,
    _bcuid: String ,
    _pri_key: String = "",
    _v_address: String = "",
    _loc_id:String = "",    
    _loc_gwuris:String="",
) extends Node with OLog {

  def uri(): String = _uri
  def uris(): Array[String] = Array(_uri);

  def name(): String = _name
  def node_idx(): Int = _node_idx;
  def bcuid(): String = _bcuid;
  def pub_key(): String = _pub_key;
  def pri_key(): String = _pri_key
  def v_address(): String = if (StringUtils.isBlank(_v_address)) _bcuid else _v_address;
  def counter(): CCSet = _counter
  def startup_time(): Long = _startup_time
  def sign(): String = _sign
  def try_node_idx(): Int = _try_node_idx
  def loc_id():String = _loc_id;
  def loc_gwuris():String = _loc_gwuris;

  override def processMessage(gcmd: String, body: Either[Message, ByteString])(implicit network: Network): Unit = {
    MessageSender.postMessage(gcmd, body, this)
  }

  override def toString(): String = {
    "PNode(" + uri + "," + startup_time + "," + pub_key + "," + node_idx +","+loc_id+ "," + sign + ")@" + this.hashCode()
  }

  override def changeIdx(idx: Int): PNode = PNode.signNode(
    name, idx, //node info
    uri, //
    startup_time, //
    pub_key, //
    counter,
    try_node_idx,
    bcuid,
    pri_key,
    v_address, sign,loc_id,loc_gwuris)

  override def changeVaddr(vaddr: String): Node = PNode.signNode(
    name, node_idx, //node info
    uri, //
    startup_time, //
    pub_key, //
    counter,
    try_node_idx,
    bcuid,
    pri_key,
    vaddr,"",loc_id,loc_gwuris)

  override def changeName(newname: String): Node = PNode.signNode(
    newname, node_idx, //node info
    uri, //
    startup_time, //
    pub_key, //
    counter,
    try_node_idx,
    bcuid,
    pri_key,
    v_address,"",loc_id,loc_gwuris)
}

object PNode extends OLog{
  def fromURL(url: String, netid: String,loc_id:String,loc_gwuris:String): PNode = {
    val u = new URL(url);
    val n = new PNode(_name = u.getHost, _node_idx = 0, "", _uri = u.toString(),
      _bcuid = Base64.encodeBase64URLSafeString((url + "?netid=" + netid).getBytes),
      _pub_key = "",_loc_id=loc_id,_loc_gwuris=loc_gwuris)
    //println("pNode.fromURL="+n._bcuid);    
    n
  }

  val NoneNode: PNode = PNode(_name = "", _node_idx = 0, _sign = "",_loc_id="",_bcuid="")

  def signNode(name: String, node_idx: Int, //node info
    uri: String = "", //
    startup_time: Long = System.currentTimeMillis(), //
    pub_key: String = null, //
    counter: CCSet = CCSet(),
    try_node_idx: Int = 0,
    bcuid: String ,
    pri_key: String = null,
    v_address: String,
    signed: String = "",loc_id:String="",loc_gwuris:String=""): PNode = {
    if (StringUtils.isNotBlank(pri_key)) {
      PNode(name, node_idx, Daos.enc.bytesToHexStr(Daos.enc.sign(Daos.enc.hexStrToBytes(pri_key), Array(bcuid, v_address,loc_id).mkString("|").getBytes)),
        uri, //
        startup_time, //
        pub_key, //
        counter,
        try_node_idx,
        bcuid,
        pri_key, v_address,loc_id,loc_gwuris)
    } else {
      PNode(name, node_idx, signed,
        uri, //
        startup_time, //
        pub_key, //
        counter,
        try_node_idx,
        bcuid,
        pri_key, v_address,loc_id,loc_gwuris)
    }
  }

  val prop: PropHelper = new PropHelper(null);

  def genIdx(newidx: Int = -1): Int = {
    var currentidx: Int = newidx
    while (currentidx <= 0) {
      currentidx = (Math.abs(Math.random() * 100000 % prop.get("otrans.node.max_nodes", 256))).asInstanceOf[Int];
    }
    val d = prop.get("otrans.node.idx", "" + currentidx);
    val envid = System.getProperty("otrans.node.idx", d);
    try {
      Integer.parseInt(NodeHelper.envInEnv(envid));
    } catch {
      case t: Throwable =>
        log.warn("error in genIdx:"+t,t);
        currentidx
    }
  }
}

case class ClusterNode(net_id: String, root_name: String, cnode_idx: Int, //node info
    _sign: String = "",

    pnodes: Array[Node],
    _counter: CCSet = CCSet(),
    _startup_time: Long = System.currentTimeMillis(),
    _try_cnode_idx: Int = 0,
    _net_bcuid: String,
    _pub_key: String = "",
    _pri_key: String = "",
    _v_address: String = "",
    _uri: String = "" ,
    _loc_id:String = "",
    _loc_gwuris:String=""
    ) extends Node with OLog {

  var masternode: Node = pnodes(0);

  override def processMessage(gcmd: String, body: Either[Message, ByteString])(implicit network: Network): Unit = {
    MessageSender.postMessage(gcmd, body, masternode)
  }

  override def toString(): String = {
    "ClusterNode(" + net_id + "," + root_name + "," + startup_time + "," + cnode_idx +","+loc_id+ "," + sign + ")@" + this.hashCode()
  }

  def uri(): String = pnodes.foldLeft("")((A, n) => A + n.uri + ",");
  def uris(): Array[String] = {
    pnodes.map { n => n.uri }
  }

  def name(): String = root_name
  def node_idx(): Int = cnode_idx;
  def bcuid(): String = _net_bcuid;
  def pub_key(): String = _pub_key;
  def pri_key(): String = _pri_key
  def counter(): CCSet = _counter
  def startup_time(): Long = _startup_time
  def sign(): String = _sign
  def v_address(): String = if (StringUtils.isBlank(_v_address)) _net_bcuid else _v_address;
  def try_node_idx(): Int = _try_cnode_idx
  def loc_id():String = _loc_id;
  def loc_gwuris():String=_loc_gwuris;
  def signNode(): ClusterNode =
    ClusterNode(
      net_id, root_name, node_idx, //node info
      if (StringUtils.isNotBlank(pri_key)) {
        Daos.enc.bytesToHexStr(Daos.enc.sign(Daos.enc.hexStrToBytes(pri_key), Array(bcuid, v_address,loc_id).mkString("|").getBytes)) //
      } else {
        sign
      }, pnodes, counter,
      startup_time, //
      try_node_idx,
      bcuid,
      pub_key,
      pri_key,
      v_address(),
      uri(), //
      loc_id(),
      loc_gwuris()
      )
  override def changeIdx(idx: Int): Node = {
    ClusterNode(
      net_id, root_name, idx, //node info
      if (StringUtils.isNotBlank(pri_key)) {
        Daos.enc.bytesToHexStr(Daos.enc.sign(Daos.enc.hexStrToBytes(pri_key), Array(idx, bcuid, v_address,loc_id).mkString("|").getBytes)) //
      } else {
        sign
      }, pnodes, counter,
      startup_time, //
      idx,
      bcuid,
      pub_key,
      pri_key,
      v_address(),
      uri(), //
      loc_id(),
      loc_gwuris()
      )
  }
  override def changeVaddr(vaddr: String): Node = {
    ClusterNode(
      net_id, root_name, node_idx, //node info
      if (pri_key != null) {
        Daos.enc.bytesToHexStr(Daos.enc.sign(Daos.enc.hexStrToBytes(pri_key), Array(node_idx, bcuid, vaddr,loc_id).mkString("|").getBytes)) //
      } else {
        sign
      }, pnodes, counter, //
      startup_time, //
      node_idx(),
      bcuid,
      pub_key,
      pri_key,
      vaddr,
      uri(), //
      loc_id(),
      loc_gwuris()
      )
  }

  override def changeName(name: String): Node = {
    ClusterNode(
      net_id, name, node_idx, //node info
      if (StringUtils.isNotEmpty(pri_key)) {
        Daos.enc.bytesToHexStr(Daos.enc.sign(Daos.enc.hexStrToBytes(pri_key), Array(node_idx, bcuid, v_address,loc_id).mkString("|").getBytes)) //
      } else {
        sign
      }, pnodes, counter, //
      startup_time, //
      _try_cnode_idx,
      bcuid,
      pub_key,
      pri_key,
      v_address,
      uri(), //
      loc_id(),
      loc_gwuris()
      )
  }
}

object ClusterNode {

  val prop: PropHelper = new PropHelper(null);

  def genIdx(newidx: Int = -1): Int = {
    var currentidx: Int = newidx
    if (currentidx == -1) {
      currentidx = (Math.abs(Math.random() * 100000 % prop.get("otrans.node.max_nodes", 256))).asInstanceOf[Int];
    }
    val d = prop.get("otrans.cnode.idx", "" + currentidx);
    val envid = System.getProperty("otrans.cnode.idx", d);
    try {
      Integer.parseInt(NodeHelper.envInEnv(envid));
    } catch {
      case _: Throwable =>
        currentidx
    }
  }

}





