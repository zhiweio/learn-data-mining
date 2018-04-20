package me.zhiwei

//import java.io.File
//import sys.process._

import java.util.Date
import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
import scala.collection.mutable
import org.apache.hadoop
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object FDMiner {

  def decompose(tree: TreeNode, r: Int): mutable.Set[List[String]] = {
    val subs = mutable.Set[List[String]]()
    val properties: ArrayBuffer[String] = tree.left
    val rhs: String = tree.right
    for ( lhs <- properties.combinations(r) ) {
      lhs.append(rhs)
      subs.add(lhs.toList)
    }

    subs
  }

  def mine(t: FDTreeX, heads: Array[String], dataSet: Array[String]): ArrayBuffer[TreeNode] = {
    val fds = ArrayBuffer[TreeNode]()
    val unibots: ArrayBuffer[TreeNode] = t.unibottom()
//    println("[DEBUG] unibots len: %d".format(t.unibottom().length))

    for ( trnode <- unibots ) {
      val fd = new FDetector(heads, dataSet, trnode)
      val found: Boolean = fd.detect()
      if ( !found ) {
        t.prune(trnode)
      } else {
        fds.append(trnode)
      }
    }

//    println("[DEBUG] unibots len: %d".format(t.unibottom().length))
    while (t.ispanning()) {
//      println("[DEBUG] ispanning: %s".format(t.ispanning()))
      t.spanning()
      val unibots: ArrayBuffer[TreeNode] = t.unibottom()
//      println("[DEBUG] unibots len: %d".format(t.unibottom().length))
      for ( trnode <- unibots ) {
        val fd = new FDetector(heads, dataSet, trnode)
        val found: Boolean = fd.detect()
        if ( !found ) {
          t.prune(trnode)
        } else {
          fds.append(trnode)
        }
      }
    }

    fds
  }

  def mineParallelize(num: Int, t: FDTreeX, heads: Array[String], dataSet: RDD[String]): ArrayBuffer[TreeNode] = {
    val fds = ArrayBuffer[TreeNode]()
    val unibots: ArrayBuffer[TreeNode] = t.unibottom()

    for ( trnode <- unibots ) {
      val found: Boolean = FDetector.detect(heads, dataSet, trnode)
      if ( !found ) {
        t.prune(trnode)
      } else {
        fds.append(trnode)
      }
    }

    while (t.ispanning()) {
      t.spanning()
      val unibots: ArrayBuffer[TreeNode] = t.unibottom()
      /* split nodes to blocks */
      val block = unibots.sliding(num, num)
      /* store result of task in queue */
      val queue = ArrayBuffer[Future[(Boolean, TreeNode)]]()
      /* build thread pool */
      val pool: ExecutorService = Executors.newFixedThreadPool(num)
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
        if ( !x._1 ) {
          t.prune(x._2)
        } else {
          fds.append(x._2)
        }
      })
    }

    fds
  }

  def minine(fds: ArrayBuffer[TreeNode], level: Int): ArrayBuffer[TreeNode] = {
    /*
    * level = t.maxLevel - 1 */
    val fdx: Array[TreeNode] = fds.toArray
    val fdmins = ArrayBuffer[TreeNode]()
    var count: Int = 1
    while ( count <= level ) {
      val bottom = mutable.Set[List[String]]()
      for ( x <- fdx ) {
          if ( x.length == count ) {
            val lhs: List[String] = x.left.toList
            val rhs: String = x.right
            bottom.add(lhs:+rhs)
         }
      }
      /*
      * nodes of upper tier, length of per node > now count */
      val upper: ArrayBuffer[TreeNode] = for ( x <- fds; if x.left.length > count ) yield x

      for ( trnode <- upper ) {
        if ( decompose(trnode, count).intersect(bottom).isEmpty && trnode.length == count+1 ) {
            fdmins.append(trnode)
        }
      }
      count += 1
    }

    fdmins
  }

  def merge(fdmins: ArrayBuffer[TreeNode]): mutable.Set[String] = {
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

    for ( (k, v) <- records ) {
      /*
       * print log on screen and output to hdfs */
      println("%s: %s".format(k.mkString("[", ", ", "]"), v))
      val s: String =  "%s: %s".format(k.mkString("[", ", ", "]"), v)
      saved.add(s)
    }
    println("total: %d".format(fdmins.length))
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

  def execute(threadsNum: Int, thresholdSize: Int, readPath: String, writePath: String ): Unit = {
    /*
    * @param threadsNum: number of concurrent jobs to submit
    * @param thresholdSize: set file size for strategy selection (M) */
    val conf = new SparkConf().setAppName("FFDMin")
    val sc = new SparkContext(conf)

    val rawSet: RDD[String] = sc.textFile(readPath)
    val heads: Array[String] = utils.headsOfDataSet(rawSet)
    val t = new FDTreeX(heads.to[ArrayBuffer])

    var fds = ArrayBuffer[TreeNode]()
    /* strategy selection depends file size */
    val fileSize: Long = hdfsFileSize(readPath)
    if ( fileSize < thresholdSize * 1024 * 1024 ) {
      /* local mode */
      val dataSet: Array[String] = rawSet.collect()
      fds = mine(t, heads, dataSet)
    }
    else {
      /* concurrently submit spark jobs */
      fds = mineParallelize(threadsNum, t, heads, rawSet)
    }

    val level: Int = t.maxLevel - 1
    val fdmins = minine(fds, level)
    fdmins ++= t.unibottom()

    val saved: Array[String] = merge(fdmins).toArray
    val records: RDD[String] = sc.parallelize(saved)
    /* [local test] remove dir if exist */
    /*
    val dir = new File(writePath)
    if (dir.exists && dir.isDirectory) {
      Seq("rm", "-rf", writePath).!!
    } */
    /* output to local or HDFS */
    records.saveAsTextFile(writePath)

    sc.stop()
//    println("[DEBUG] Spark Application finished")
  }

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val outputPath = args(1)
//    val tempath = args(2)

    /* @deprecate command line arguments parse
    args.sliding(2,2).toList.collect {
      case Array("--input", argInput: String) => inputPath = argInput
      case Array("--output", argOutput: String) => outputPath = argOutput
      case Array("--tempath", argTemp: String) => tempath = argTemp
    } */
    println("[INFO] start at - %s".format(new Date()))
    execute(50, 1, inputPath, outputPath)
    println("[INFO] finished at - %s".format(new Date()))
  }

}

