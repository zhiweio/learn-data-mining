package me.zhiwei

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable


class FDTree(properties: Array[String]) {

  val maxLevel: Int = properties.length - 1
  var spanningLevel: Int = 0
  var lhsSize: Int = 1
  private val branches = ArrayBuffer[TreeNode]()
  val nontrivialMinNodes =  mutable.Set[TreeNode]()


  for( i <- properties.indices ) {
    val lhs = properties.to[ArrayBuffer].clone()
    val rhs = properties(i)
    lhs.remove(i)
    val node = new TreeNode(lhs.toSet, rhs)
    branches.append(node)
  }

  spanningLevel += 1
  var bottom = ArrayBuffer[TreeNode]()

  override def toString: String =
    "<FDTree>\nmax level: %d\nspanning level: %d\nbranches: %s".format(
      maxLevel, spanningLevel, branches)

  def bottomNodes(): Set[TreeNode] = {
    /** 清除节点，lhs 为 nontrivialMinNodes 中节点的 lhs 超集的
      * 通过调用 bottomNode() 得到需要检测是否为依赖的节点 */

    if ( bottom.isEmpty || bottom.last.left.size == 1 ) bottom.toSet
    /** NOTE: 第一次生成后，节点 lhs 为单属性，无需与 nontrivialMinNodes 中进行匹配*/
    if ( spanningLevel == 1 ) Set[TreeNode]() /** NOTE: only branches contained in bottom */
    if ( nontrivialMinNodes.isEmpty ) bottom.toSet

    bottom.filter(x => !nontrivialMinNodes.map(
      a => a.right == x.right && a.left.subsetOf(x.left)).contains(true)).toSet
  }

  /** 初始化时已经生成出了 size 为 maxLevel - 1 的 lhs */
  def ispanning(): Boolean = spanningLevel < maxLevel && lhsSize < maxLevel - 1

  def spanning(): Unit = {
    if (!ispanning()) {
      throw new IllegalArgumentException("Can not span, exceed spanningLevel limit")
    }
    spanningLevel += 1

//    println("[INFO] spanningLevel: %d".format(spanningLevel))
//    println("[INFO] lhsSize: %d".format(lhsSize))
//    println("[DEBUG] branches size: %d".format(branches.length))
    val bottle = ArrayBuffer[TreeNode]()
    for ( node <- branches ) {
      val rhs: String = node.right
      for ( x <- node.left.toArray.combinations(lhsSize).toList ) {
        bottle.append(new TreeNode(x.toSet, rhs))
      }
    }
    /** NOTE: nodes contained in bottom have the same lhs size*/
    bottom = bottle
//    println("[DEBUG] bottom len: %d".format(bottom.length))

    lhsSize += 1
  }

  def spanningAll(): Unit = {
    while ( ispanning() ) spanning()
  }

}

object testFDtree extends App {

  var t = new FDTree(Array[String]("A", "B", "C", "D", "E"))
  val fds = mutable.Set[TreeNode]()
  val nondepend = Set[TreeNode](new TreeNode(Set("A"), "B"), new TreeNode(Set("A"), "C"))
  while ( t.ispanning() ) {
    t.spanning()
    val nodes = t.bottomNodes()
    for ( node <- nodes ) {
      if ( nondepend.contains(node) ) {
        t.nontrivialMinNodes.add(node)
        fds.add(node)
      }
//      println("[DEBUG] prune: %s -> %s".format(node.left.mkString("[", ", ", "]"), node.right))
    }
    println("spanningLevel: %d".format(t.spanningLevel))
    println("bots len: %d".format(t.bottomNodes().size))
    println("nontrivialMinNodes len: %d".format(t.nontrivialMinNodes.size))
  }
  fds.foreach(println)
}