package me.zhiwei

import java.util.Date
import org.apache.spark.rdd.RDD


object utils {

  def headsOfDataSet(dataSet: RDD[String]): Array[String] =
  {
    val headline: String = dataSet.first()
    val count: Int = headline.trim.split(",").length
    val heads = for (i <- 1 to count) yield "column"+i
    heads.toArray
  }


}

object testUtil extends App {
  override def main(args: Array[String]): Unit = {
    println("start at - %s".format(new Date()))
  }
}

