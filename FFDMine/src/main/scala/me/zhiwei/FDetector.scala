package me.zhiwei

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class FDetector(heads: Array[String], dataSet: Array[String], node: TreeNode) {

  private var items = Array[(String, String)]()

  def products(): Array[(String, String)] = {
    if ( !node.isInstanceOf[TreeNode] ) {
      println("[ERROR] illegal tree node: %s".format(node))
      throw new IllegalArgumentException("Illegal TreeNode")
    }

    val lhs_properties: ArrayBuffer[String] = node.left
    val rhs_properity: String = node.right
    val index: Map[String, Int] = heads.zipWithIndex.toMap

    for { line <- dataSet
         fields: Array[String] = line.trim.split(",")
         lhs: Array[String] = for ( x <- lhs_properties.toArray ) yield fields(index.getOrElse(x, 0))
         rhs: String = fields(index.getOrElse(rhs_properity, 0))
    } yield (lhs.mkString(","), rhs.toString)

  }

  def detect(): Boolean = {
    val fds = mutable.HashMap[String, String]()
    items = products()

    for ( x <- items; lhs = x._1; rhs = x._2 ) {
      if ( !fds.contains(lhs) ) {
        fds.put(lhs, rhs)
      } else {
        if ( fds.getOrElse(lhs, "") != rhs ) {
//          println("[DEBUG] conflict: %s -> %s rhs: %s".format(lhs, fds.getOrElse(lhs, ""), rhs))
          return false
        }
      }
    }
    true
  }

}

object FDetector {

  private val NDFLAG: String = "NON_DEPENDENCY_FLAG"

  def detect(heads: Array[String], dataSet: RDD[String], node: TreeNode): Boolean = {
    if ( !node.isInstanceOf[TreeNode] ) {
      println("[ERROR] illegal tree node: %s".format(node))
      throw new IllegalArgumentException("Illegal TreeNode")
    }

    val lhsProperties: Array[String] = node.left.toArray
    val rhsProperity: String = node.right
    val index: Map[String, Int] = heads.zipWithIndex.toMap

    /* @deprecate
    def convert2pairs(row: String): (String, String) = {
      val fields: Array[String] = row.trim.split(",")
      val lhs: Array[String] = for ( x <- lhsProperties; posi = index.getOrElse(x, 0) )
        yield fields(posi)
      val rhs: String = fields(index.getOrElse(rhsProperity, 0))
      (lhs.mkString(","), rhs)
    } */

    val pairs = dataSet.map(line => {
      val fields: Array[String] = line.trim.split(",")
      val lhs: Array[String] = for ( x <- lhsProperties; pos = index.getOrElse(x, 0) )
        yield fields(pos)
      val rhs: String = fields(index.getOrElse(rhsProperity, 0))
      (lhs.mkString(","), rhs)
    })
    val groups = pairs.groupByKey().mapValues(vals => vals.toSet.size)
    val conflict = groups.filter(x => x._2 > 1)
    val found: Boolean = conflict.isEmpty()

    found
  }

  def detectx(heads: Array[String], dataSet: RDD[String], node: TreeNode): Boolean = {
    if (!node.isInstanceOf[TreeNode]) {
      println("[ERROR] illegal tree node: %s".format(node))
      throw new IllegalArgumentException("Illegal TreeNode")
    }

    val lhsProperties: Array[String] = node.left.toArray
    val rhsProperity: String = node.right
    val index: Map[String, Int] = heads.zipWithIndex.toMap

    val pairs = dataSet.map(line => {
      val fields: Array[String] = line.trim.split(",")
      val lhs: Array[String] = for (x <- lhsProperties; pos = index.getOrElse(x, 0))
        yield fields(pos)
      val rhs: String = fields(index.getOrElse(rhsProperity, 0))
      (lhs.mkString(","), rhs)
    })
    val found: Boolean = pairs.reduceByKey {
      case (a, b) if a == b => a
      case (a, b) if a != b => NDFLAG
    }.filter(_._2 == NDFLAG).isEmpty()

    found
  }

}
