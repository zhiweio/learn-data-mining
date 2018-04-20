package me.zhiwei

import scala.util.hashing.MurmurHash3


class TreeNode(leftHandSide: Set[String], rightHandSide: String) {
  val left = leftHandSide
  val right = rightHandSide

  def length = left.size

  def canEqual(a: Any) = a.isInstanceOf[TreeNode]

  override def equals(that: Any): Boolean =
    that match {
      case that: TreeNode => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode(): Int = {
    val hash: Int = MurmurHash3.setHash(left) + MurmurHash3.stringHash(right)
    hash
  }

  override def toString: String = {
    "<TreeNode> - left: %s -> right: %s".format(this.left.mkString(", "), this.right)
  }

}
