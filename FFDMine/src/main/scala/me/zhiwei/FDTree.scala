package me.zhiwei

import scala.collection.mutable.ArrayBuffer

object FDTree {

  def findSubtrees( leaves: ArrayBuffer[TreeNode] ): ArrayBuffer[TreeNode] = {
    val sub = for ( leaf <- leaves; trees = leaf.subtrees; node <- trees) yield node
    sub.distinct
  }

}


class FDTree(properties: ArrayBuffer[String]) {

  val maxLevel: Int = properties.length - 1
  var spanningLevel: Int = 0
  var root = new TreeNode(properties, "", ArrayBuffer[TreeNode]())

  for( i <- properties.indices ) {
    val lhs = properties.clone()
    val rhs = properties(i)
    lhs.remove(i)
    val trnode = new TreeNode(lhs, rhs, ArrayBuffer[TreeNode]())
    root.subtrees.append(trnode)
  }

  spanningLevel += 1
  var bottomSubtrees: ArrayBuffer[TreeNode] = root.subtrees

  override def toString: String =
    "<FDTree>\nmax level: %d\nspanning level: %d\n%s".format(
      maxLevel, spanningLevel, root)

  def unibottom(): ArrayBuffer[TreeNode] = {
    bottomSubtrees.distinct
  }

  def ispanning(): Boolean = spanningLevel < maxLevel

  def spanning(): Unit = {
    if ( !ispanning() ) {
      throw new IllegalArgumentException("can not spanning")
    }

    val bottoms: ArrayBuffer[TreeNode] = unibottom()
    for ( node <- bottoms ) {
      val properties: ArrayBuffer[String] = node.left
      val rhs: String = node.right
      for ( i <- properties.indices ) {
        val lhs: ArrayBuffer[String] = properties.clone()
        lhs.remove(i)
        val trnode: TreeNode = new TreeNode(lhs, rhs, ArrayBuffer[TreeNode]())
        node.subtrees.append(trnode)
      }
    }

    bottomSubtrees = FDTree.findSubtrees(bottoms)
    spanningLevel += 1
    println("lv%d - in spanning bots len: %d".format(spanningLevel, bottomSubtrees.length))
  }

  def spanningAll(): Unit = {
    while ( ispanning() ) spanning()
  }

  def prune( treenode: TreeNode ): Boolean = {
//    println("[DEBUG] prune: %s -> %s".format(treenode.left.mkString(", "), treenode.right))

    val trees:ArrayBuffer[TreeNode] = bottomSubtrees
//    println("[DEBUG] before prunning bots len: %d".format(trees.length))
    val indices = ArrayBuffer[Int]()
    for ( node <- trees ) {
      if ( node == treenode )
        indices.append(trees.indexOf(node))
    }
    if ( indices.nonEmpty ) {
      indices.map(trees.remove)
      bottomSubtrees = trees
//      println("[DEBUG] after prunning bots len: %d".format(bottomSubtrees.length))
      true
    }
    else {
      false
    }

  }
}

object testFDtree extends App {
  var t = new FDTree(ArrayBuffer[String]("A", "B", "C", "D", "E"))
  t.root.subtrees.foreach(println(_))
//  t.spanning()
//  var bots = t.unibottom()
//  println(bots.length)
//  for (node <- bots.take(50)) t.prune(node)
//  t.spanning()
//  t.spanning()
//  t.spanning()
  while ( t.ispanning() ) {
    t.spanning()
    val unibots = t.unibottom()
    for ( node <- unibots ) {
      println("[DEBUG] prune: %s -> %s".format(node.left.mkString("[", ", ", "]"), node.right))
    }
    println("uni bots len: %d".format(t.unibottom().length))
    println("spanningLevel: %d".format(t.spanningLevel))
  }
}