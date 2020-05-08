package org.mos.p22p.node.router

sealed trait IntNode

case class EmptySet() extends IntNode
case class FullNodeSet() extends IntNode
//only one layer ,for Random Node Router 
case class FlatSet(fromIdx: Int, nextHops: BigInt = BigInt(0)) extends IntNode
// Tree Router , form Circle Node Router
case class NodeSet(nodes: scala.collection.mutable.Set[IntNode]) extends IntNode

case class DeepTreeSet(fromIdx: Int, treeHops: NodeSet) extends IntNode 
 

