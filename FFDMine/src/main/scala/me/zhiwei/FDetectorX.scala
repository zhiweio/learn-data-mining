package me.zhiwei

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class FDetectorX {

}

object FDetectorX {
  private val NDFLAG: String = "NON_DEPENDENCY_FLAG"
  def detect(heads: Array[String], dataSet: RDD[String], node: TreeNode): Boolean = {
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


object testDetect extends App {

  override def main(args: Array[String]): Unit = {
    val readPath = "/Users/wangzhiwei/IdeaProjects/FDmining/data/bots_200_5.csv"
    val conf = new SparkConf().setAppName("FuncDependMining").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rawSet: RDD[String] = sc.textFile(readPath)
    val heads = Array("column1", "column2", "column3", "column4", "column5")
    val node = new TreeNode(ArrayBuffer("column3"), "column5", ArrayBuffer[TreeNode]())
    val isdenpend = FDetectorX.detect(heads, rawSet, node)
    println("RESULT:")
    println(isdenpend)
  }

}
