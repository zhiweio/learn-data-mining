package me.zhiwei


import java.util.Date
import java.util.concurrent._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Miner {

  def fakeMine(trick: Boolean, accuracy: Int, t: FDTree,
               heads: Array[String], dataSet: Array[String]): mutable.Set[TreeNode] = {
    var executable: Boolean = t.ispanning()
    var N = accuracy
    val fds = mutable.Set[TreeNode]()

    while ( executable ) {
      t.spanning()
      val nodes = t.bottomNodes()
//      println("[DEBUG] nodes to detect: %d\n".format(nodes.size))
      for ( trnode <- nodes ) {
        val fd = new FDetector(heads, dataSet, trnode)
        val found: Boolean = fd.detect()
        if ( found ) {
          t.nontrivialMinNodes.add(trnode)
          fds.add(trnode)
        }
      }
      if ( trick ) {
         N -= 1
         executable = N  > 0  && t.ispanning()
//        println("[DEBUG] N: %d".format(N))
//        println("[DEBUG] executable: %s".format(executable))
      }
      else {
         executable = t.ispanning()
      }
//      println("[DEBUG] nontrivialMinNodes len: %d".format(t.nontrivialMinNodes.size))
    }

    fds
  }

  def parallelizeMine(trick: Boolean, accuracy: Int, threadsNum: Int, t: FDTree,
                      heads: Array[String], dataSet: RDD[String]): mutable.Set[TreeNode] = {
    var executable: Boolean = t.ispanning()
    var N = accuracy
    val fds = mutable.Set[TreeNode]()

    while ( executable ) {
      t.spanning()
      val nodes = t.bottomNodes()
//      println("[DEBUG] nodes to detect: %d\n".format(nodes.size))
      /* split nodes to blocks */
      val block = nodes.sliding(threadsNum, threadsNum)
      /* store result of task in queue */
      val queue = ArrayBuffer[Future[(Boolean, TreeNode)]]()
      /* build thread pool */
      val pool: ExecutorService = Executors.newFixedThreadPool(threadsNum)
      while ( block.hasNext ) {
        for ( trnode <- block.next ) {
          val job = pool.submit(new Callable[(Boolean, TreeNode)] {
            override def call(): (Boolean, TreeNode) = {
              val found: Boolean = FDetector.detect(heads, dataSet, trnode)
              (found, trnode)
            }
          })
          queue.append(job)
        }
      }
      /* traverse results and process tree node */
      queue.foreach(found => {
        val x: (Boolean, TreeNode) = found.get()
        if ( x._1 ) {
          fds.add(x._2)
          t.nontrivialMinNodes.add(x._2)
        }
      })

      if ( trick ) {
        N -= 1
        executable = N  > 0  && t.ispanning()
      }
      else {
        executable = t.ispanning()
      }
    }

    fds
  }

  def merge(fdmins: mutable.Set[TreeNode]): mutable.Set[String] = {
    val records = mutable.HashMap[List[String], String]()
    val saved = mutable.Set[String]()
    for ( x <- fdmins ) {
      val lhs: List[String] = x.left.toList
      if ( records.contains(lhs) ) {
        val rhs: String = records.getOrElse(lhs, "")
        records.put(lhs, rhs + ", " + x.right)
      } else {
        records.put(lhs, x.right)
      }
    }
    println("\n\nresults:\n")
    for ( (k, v) <- records ) {
      /*
       * print log on screen and output to hdfs */
      println("%s: %s".format(k.mkString("[", ", ", "]"), v))
      val s: String =  "%s: %s".format(k.mkString("[", ", ", "]"), v)
      saved.add(s)
    }
    println("\ntotal: %d\n".format(fdmins.size))
    saved
  }

  def hdfsFileSize(readPath: String) = {
    val hdfs: hadoop.fs.FileSystem = hadoop.fs.FileSystem.get(
      new hadoop.conf.Configuration())
    val hadoopPath= new hadoop.fs.Path(readPath)
    val recursive = false
    val ri = hdfs.listFiles(hadoopPath, recursive)
    val file: hadoop.fs.LocatedFileStatus = ri.next()
    val fieSize: Long = file.getLen

    fieSize
  }


  def execute(threadsNum: Int, thresholdSize: Int, trick: Boolean,
              accuracy: Int, readPath: String, writePath: String ): Unit = {
    /**
      * threadsNum: number of concurrent jobs to submit
      * thresholdSize: set file size for strategy selection (M)
      * trick: use or not accuracy controller
      * accuracy:  the number of searching layer
      */
    val conf = new SparkConf().setAppName("FFDMin")
    val sc = new SparkContext(conf)

    val dataSet: RDD[String] = sc.textFile(readPath)
    val heads: Array[String] = utils.headsOfDataSet(dataSet)
    val t = new FDTree(heads)
    /** accuracy controller
    val trick = true
    val accuracy = 3
     **/

    var fds = mutable.Set[TreeNode]()
    /* strategy selection depends file size */
    val fileSize: Long = hdfsFileSize(readPath)
    if ( fileSize < thresholdSize * 1024 * 1024 ) {
      /** NOTE: local mode */
      fds = fakeMine(trick, accuracy, t, heads, dataSet.collect())
    }
    else {
      /** NOTE: concurrently submit spark jobs */
      fds = parallelizeMine(trick, accuracy, threadsNum, t, heads, dataSet)
    }

    val saved: Array[String] = merge(fds).toArray
    val records: RDD[String] = sc.parallelize(saved)
    /** output to local or HDFS */
    records.saveAsTextFile(writePath)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val trick = true
    val accuracy = 3
    val threadsNum = 50
    val thresholdSize = 1
    /* @deprecate command line arguments parse
    args.sliding(2,2).toList.collect {
      case Array("--input", argInput: String) => inputPath = argInput
      case Array("--output", argOutput: String) => outputPath = argOutput
    } */
    println("[INFO] start at - %s\n".format(new Date()))
    execute(threadsNum, thresholdSize, trick, accuracy,inputPath, outputPath)
    println("[INFO] finished at - %s".format(new Date()))
  }

}
