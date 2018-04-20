package me.zhiwei

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object FDTreeX {

  def findSubtrees(leaves: ArrayBuffer[TreeNode]): ArrayBuffer[TreeNode] = {
    val sub = for ( leaf <- leaves; trees = leaf.subtrees; node <- trees ) yield node
    sub.distinct
  }

  def reslove(node: TreeNode, nondepend: mutable.HashSet[TreeNode]): mutable.HashSet[TreeNode] = {
    if ( ! nondepend.contains(node) ) {
      var N: Int = node.left.size
      val rhs: String = node.right
      val bots = ArrayBuffer[TreeNode](node)
      val guard = 9
      var X = 3
      nondepend.add(node)

      if ( N > guard ) {
        while ( X > 0 ) {
          for (v <- bots.filter(_.left.size == N)) {
            for (u <- v.left.combinations(N - 1).toList) {
              //          println(u.mkString(","))
              val treeNode = new TreeNode(u, rhs, ArrayBuffer[TreeNode]())
              nondepend.add(treeNode)
              bots.append(treeNode)
            }
          }
          X -= 1
          N -= 1
        }
      } else {
        while (N > 1) {
          for (v <- bots.filter(_.left.size == N)) {
            for (u <- v.left.combinations(N - 1).toList) {
              val treeNode = new TreeNode(u, rhs, ArrayBuffer[TreeNode]())
              nondepend.add(treeNode)
              bots.append(treeNode)
            }
          }
          N -= 1
        }
      }
    }
    nondepend
  }

}


class FDTreeX(properties: ArrayBuffer[String]) {

  val maxLevel: Int = properties.length - 1
  var spanningLevel: Int = 0
  val root = new TreeNode(properties, "", ArrayBuffer[TreeNode]())


  for( i <- properties.indices ) {
    val lhs = properties.clone()
    val rhs = properties(i)
    lhs.remove(i)
    val trnode = new TreeNode(lhs, rhs, ArrayBuffer[TreeNode]())
    root.subtrees.append(trnode)
  }

  spanningLevel += 1
  var bottomSubtrees: ArrayBuffer[TreeNode] = root.subtrees
  val nondepend = mutable.HashSet[TreeNode]()
//  println("[DEBUG] init nondepend: %d".format(nondepend.size))

  override def toString: String =
    "<FDTreeX>\nmax level: %d\nspanning level: %d\n%s".format(
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
        if ( ! nondepend.contains(trnode) )
          node.subtrees.append(trnode)
      }
    }

    bottomSubtrees = FDTreeX.findSubtrees(bottoms)
    spanningLevel += 1
    println("lv%d - in spanning bots len: %d".format(spanningLevel, bottomSubtrees.length))
  }

  def spanningAll(): Unit = {
    while ( ispanning() ) spanning()
  }

  def prune( treenode: TreeNode ): Boolean = {
//    println("[DEBUG] prune: %s -> %s".format(treenode.left.mkString(", "), treenode.right))
    FDTreeX.reslove(treenode, nondepend)
//    println("[DEBUG] non-dependence: %d".format(nondepend.size))

    val trees:ArrayBuffer[TreeNode] = bottomSubtrees
//    println("[DEBUG] before prunning bots len: %d".format(trees.length))
    val indices = ArrayBuffer[Int]()
    for ( node <- trees ) {
      if ( node == treenode )
        indices.append(trees.indexOf(node))
    }
    if ( indices.nonEmpty ) {
      indices.map(trees.remove)
      bottomSubtrees = trees.toSet.diff(nondepend).to[ArrayBuffer]
//      println("[DEBUG] after prunning bots len: %d".format(bottomSubtrees.length))
      true
    }
    else {
      false
    }

  }
}

object testFDtreeX extends App {
  val node = new TreeNode(ArrayBuffer[String]("A", "B", "C"), "E", ArrayBuffer[TreeNode]())

  /**
    * DEBUG: test function reslove()
  val nondepend = mutable.HashSet[TreeNode]()
  FDTreeX.reslove(node, nondepend)
  println("[DEBUG] nondepend: %d".format(nondepend.size))
  nondepend.foreach(println)
    */


  var t = new FDTreeX(ArrayBuffer[String]("A", "B", "C", "D", "E"))
//  t.root.subtrees.foreach(println(_))
//  t.spanning()
//  t.prune(new TreeNode(ArrayBuffer[String]("A", "B", "C"), "E", ArrayBuffer[TreeNode]()))
//  t.prune(new TreeNode(ArrayBuffer[String]("A", "B", "C"), "D", ArrayBuffer[TreeNode]()))
//  t.prune(new TreeNode(ArrayBuffer[String]("B", "C", "D"), "A", ArrayBuffer[TreeNode]()))
  while ( t.ispanning() ) {
    t.spanning()
    t.prune(t.unibottom()(1))
//    val unibots = t.unibottom()
//    for ( node <- unibots ) {
//      t.prune(node)
//      println("[DEBUG] node: %s -> %s".format(node.left.mkString("[", ", ", "]"), node.right))
//    }
//    println("spanningLevel: %d".format(t.spanningLevel))
//    println("bots len: %d".format(t.unibottom().length))
//    t.unibottom().foreach(println)
  }
  t.nondepend.foreach(node => println("%s -> %s".format(node.left.mkString("[", ", ", "]"), node.right)))

}