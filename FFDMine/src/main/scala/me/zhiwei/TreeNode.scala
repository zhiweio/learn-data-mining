package me.zhiwei

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

class TreeNode(leftSide: ArrayBuffer[String], rightSide: String, sub: ArrayBuffer[TreeNode]) {
  val left: ArrayBuffer[String] = leftSide
  val right: String = rightSide
  var subtrees: ArrayBuffer[TreeNode] = sub

  def length = left.length

  def canEqual(a: Any) = a.isInstanceOf[TreeNode]

  override def equals(that: Any): Boolean =
    that match {
      case that: TreeNode => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode(): Int = {
    val result: Int = MurmurHash3.arrayHash(left.toArray) + MurmurHash3.stringHash(right)
    result
  }

  override def toString: String = {
    "(TreeNode  left: %s  right: %s    subtrees: %s)".format(
      this.left, this.right, this.subtrees)
  }

}

object testNode extends App {
  var n1 = new TreeNode(ArrayBuffer("1", "2"), "3", ArrayBuffer[TreeNode]())
  println(n1.left)
  println(n1.subtrees)
}
