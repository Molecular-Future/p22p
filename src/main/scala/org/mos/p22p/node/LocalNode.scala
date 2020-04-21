package org.mos.p22p.node

import onight.tfw.mservice.NodeHelper
import org.apache.commons.lang3.StringUtils
//import org.spongycastle.util.encoders.Hex
//import org.ethereum.crypto.HashUtil
import onight.oapi.scala.traits.OLog
import org.mos.mcore.crypto.KeyPairs

trait LocalNode extends OLog with PMNodeHelper with LogHelper {
  //  val node_name = NodeHelper.getCurrNodeName
  def netid(): String

  def PROP_NODE_INFO = "org.bc.pzp." + netid() + ".node.info";

  private var rootnode: Node = PNode.NoneNode;

  def root(): Node = rootnode;

  def isLocalNode(node: Node): Boolean = {
    node == root || root.bcuid.equals(node.bcuid)
  }

  def changeRootName(name: String): Unit = {
    rootnode = rootnode.changeName(name);
    syncInfo(rootnode);
    MessageSender.sockSender.setCurrentNodeName(rootnode.bcuid)
  }

  def isLocalNode(bcuid: String): Boolean = {
    root.bcuid.equals(bcuid)
  }

  def getFromDB(key: String, defaultv: String): String = {
    val v = Daos.odb.get(key.getBytes)
    if (v == null || v.get() == null) {
      val prop = NodeHelper.getPropInstance.get(key, defaultv);
      NodeHelper.envInEnv(prop)
    } else {
      new String(v.get)
    }
  }

  def syncInfo(node: Node): Boolean = {
    if (Daos.odb == null) return false;
    Daos.odb.put(PROP_NODE_INFO.getBytes, serialize(node).getBytes);
    true
  }
  def newNode(nodeidx: Int = -1): PNode = {
    val kp = Daos.enc.genAccountKey()
    val newroot = PNode.signNode(kp.getBcuid, node_idx = -1,
      uri = "tcp://" + NodeHelper.getCurrNodeListenOutAddr + ":" + NodeHelper.getCurrNodeListenOutPort,
      System.currentTimeMillis(), pub_key = kp.getPubkey,
      try_node_idx = nodeidx,
      bcuid = netid().head.toUpper + kp.getBcuid,
      pri_key = kp.getPrikey,
      v_address = kp.getAddress,
      signed = "",
      loc_id = Config.LOC_ID,
      loc_gwuris = Config.LOC_GWURIS)
    syncInfo(newroot)
    newroot;
  }

  def copyNode(nodeidx: Int = -1, kp: KeyPairs): PNode = {
    val newroot = PNode.signNode(kp.getBcuid, node_idx = nodeidx,
      uri = "tcp://" + NodeHelper.getCurrNodeListenOutAddr + ":" + NodeHelper.getCurrNodeListenOutPort,
      System.currentTimeMillis(), pub_key = kp.getPubkey,
      try_node_idx = nodeidx,
      bcuid = netid().head.toUpper + kp.getBcuid,
      pri_key = kp.getPrikey,
      v_address = kp.getAddress,
      signed = "",
      loc_id = Config.LOC_ID,
      loc_gwuris = Config.LOC_GWURIS)
    syncInfo(newroot)
    newroot;
  }

  def initNode() = {
    this.synchronized {
      if (rootnode == PNode.NoneNode) //second entry
      {
        try {
          val nodeidx = PNode.genIdx()
          val node_info = getFromDB(PROP_NODE_INFO, "");
          rootnode =
            try {
              log.info("load node from db info:reset=" + Config.RESET_NODEINFO + ",dbv=" + node_info + ":")
              val r = if (StringUtils.isBlank(node_info) || Config.RESET_NODEINFO == 1) {
                newNode(PNode.genIdx());
              } else {
                deserialize(node_info, "tcp://" + NodeHelper.getCurrNodeListenOutAddr + ":" + NodeHelper.getCurrNodeListenOutPort)
              }
              log.info("load node from db:" + r.bcuid + ",idx=" + r.node_idx)
              r
            } catch {
              case e: Throwable =>
                val r = newNode(PNode.genIdx());
                log.debug("new node info:" + r.bcuid + ",idx=" + r.node_idx)
                r
            }

          //          if (MessageSender.sockSender != null && rootnode != null && rootnode.bcuid != null) {
          //            MessageSender.sockSender.setCurrentNodeName(rootnode.bcuid)
          //          }

        } catch {
          case e: Throwable =>
            log.warn("unknow Error.", e)
        } finally {
          log.info("rootnode=" + rootnode)
          if (root() != null) {
            MDCSetBCUID(root().bcuid)
          }
        }
      }
    }
  }
  def initClusterNode(subnetRoot: Node, rootname: String) = {
    this.synchronized {
      initNode();
      rootnode = ClusterNode(
        net_id = netid(),
        rootnode.name,
        cnode_idx = -1,
        _sign = "",
        pnodes = Array(subnetRoot),
        _net_bcuid = rootnode.bcuid,
        _try_cnode_idx = if (rootnode.try_node_idx > 0) rootnode.try_node_idx else ClusterNode.genIdx(),
        _pub_key = rootnode.pub_key,
        _pri_key = rootnode.pri_key,
        _v_address = rootnode.v_address,
        _loc_id = rootnode.loc_id,
        _loc_gwuris = rootnode.loc_gwuris).signNode();
      if (rootnode == PNode.NoneNode) //second entry
      {
        try {
          val nodeidx = PNode.genIdx()
          val node_info = getFromDB(PROP_NODE_INFO, "");
          rootnode =
            try {
              log.info("load node from db info=:" + node_info)
              val r = if (StringUtils.isBlank(node_info)) {
                newNode(PNode.genIdx());
              } else {
                deserialize(node_info)
              }
              log.info("load node from db:" + r.bcuid + ",idx=" + r.node_idx)
              r
            } catch {
              case e: Throwable =>
                val r = newNode(PNode.genIdx());
                log.debug("new node info:" + r.bcuid + ",idx=" + r.node_idx)
                r
            }

          if (MessageSender.sockSender != null && rootnode != null && rootnode.bcuid != null) {
            MessageSender.sockSender.setCurrentNodeName(rootnode.bcuid)
          }

        } catch {
          case e: Throwable =>
            log.warn("unknow Error.", e)
        } finally {
          if (root() != null) {
            if (MessageSender.sockSender != null && rootnode != null && rootnode.bcuid != null) {
              MessageSender.sockSender.setCurrentNodeName(rootnode.bcuid)
            }
            MDCSetBCUID(root().bcuid)
          }
        }
      }
    }
  }
  def resetRoot(node: Node): Unit = {
    this.rootnode = rootnode.changeIdx(node.node_idx).changeName(node.name);
    MessageSender.sockSender.setCurrentNodeName(rootnode.bcuid)
  }
  

  def changeNodeIdx(test_bits: BigInt = BigInt("0"), sugguestid: Int = 0): Int = {
    this.synchronized {
      var v = 0;
      var cc = 0;
      val envid = System.getProperty("otrans.node.idx");
      val newnode = if (sugguestid == 0) {
        do {
          v = PNode.genIdx()
          cc = cc + 1
          if (cc % 1000 == 0) {
            log.warn("gen Idx too many times:cc=" + cc + ",testbits=" + test_bits.bigInteger.toString(2) + ",tryv=" + v + ",node_idx=" + rootnode.node_idx
              + ",envid=" + envid)

          }
        } while (rootnode.node_idx == v || test_bits.testBit(v))
        newNode(v);
      } else {
        v = sugguestid;
        copyNode(v, new KeyPairs(rootnode.pub_key, rootnode.pri_key, rootnode.v_address, 
                Daos.enc.genBcuid(rootnode.pub_key)
            ))
      }

      if (rootnode.isInstanceOf[ClusterNode]) {
        log.debug("new clusternode");

        val oldrootnode = rootnode.asInstanceOf[ClusterNode];
        rootnode = ClusterNode(net_id = netid(), rootnode.name,
          cnode_idx = -1, _sign = newnode.sign(),
          pnodes = oldrootnode.pnodes,
          _net_bcuid = newnode.bcuid,
          _try_cnode_idx = newnode.try_node_idx,
          _pub_key = newnode.pub_key(),
          _pri_key = newnode.pri_key(),
          _loc_id = newnode.loc_id(),
          _loc_gwuris = newnode.loc_gwuris());
      } else {
        rootnode = newnode;
      }

      syncInfo(rootnode)
      MDCSetBCUID(root().bcuid)
      log.debug("changeNode Index=" + v)
      v
    }
  }

  def changeNodeVAddr(newaddr: String): Node = {
    this.synchronized {
      if (!StringUtils.equals(newaddr, rootnode.v_address)) {
        rootnode = rootnode.changeVaddr(newaddr)
        syncInfo(rootnode)
        MDCSetBCUID(root().bcuid)
        log.debug("changeNode VAddr=" + newaddr)
      } else {
        MDCSetBCUID(root().bcuid)
        log.debug("same node vAddr=" + newaddr)
      }

      rootnode
    }
  }

}
