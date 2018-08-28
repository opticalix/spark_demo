package com.opticalix

import com.github.nscala_time.time.Imports._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.DateTimeFormat

object Main {

  def hadoopLogAnalysis(sc: SparkContext, args: Array[String]) = {
    var hadoopLogFile = ""
    if (args != null && args.length > 0
      && (args(0).startsWith("file://") || args(0).startsWith("hdfs://"))) {
      hadoopLogFile = args(0)
    }
    println(s"hadoopLogFile=$hadoopLogFile")

    val error = sc.textFile(hadoopLogFile).filter(_.contains("ERROR")).cache()
    val errCnt = error.count()
    val mysqlErrCnt = error.filter(_.contains("MySQL")).count()
    val hdfsMsg = error.filter(_.contains("HDFS")).map(_.split("\t")(3)).collect()
    println(s"errCnt=$errCnt, mysqlErrCnt=$mysqlErrCnt, hdfsMsg=$hdfsMsg")
  }

  def arrayTest(sc: SparkContext): Unit = {
    val intArr = Array[Int](1, 2, 3, 4, 5)
    //Accumulators in Spark are used specifically to provide a mechanism for safely updating a variable when execution is split up across worker nodes in a cluster
    val acc = sc.longAccumulator
    val midResult = sc.parallelize(intArr)
      .map(x => x + 1)
      .persist(StorageLevel.MEMORY_ONLY)

    midResult.foreach(x => acc.add(x))
    val result = midResult.reduce((x, y) => x + y)
    val accSum = acc.sum
    println(s"acc=$accSum, result=$result")
  }

  def pageRank(sc: SparkContext): Unit = {
    //Define alpha
    val alpha = 0.85
    val iterCnt = 20
    //Init relation graph of pages
    val links = sc.parallelize(
      List(
        ("A", List("A", "C", "D")),
        ("B", List("D")),
        ("C", List("B", "D")),
        ("D", List()))
    )
      //Take advantage of partitions and save in mem cache
      .partitionBy(new HashPartitioner(2))
      .persist()
    //Init pageRanks
    var ranks = links.mapValues(_ => 1.0)

    //Iteration
    val tupleFunc = (_: String, linkRank:(List[String], Double)) => {
      val linkList = linkRank._1
      val rank = linkRank._2
      linkList.map(dest => (dest, rank / linkList.size))
    }
    for (i <- 0 until iterCnt) {
      val contributions = links.join(ranks)
        //1. use tupleFunc
//        .flatMap(tupleFunc)
        //2. use partialFunc
        .flatMap{
        case (_, (linkList, rank)) =>
          val tuples = linkList.map(dest => (dest, rank / linkList.size))
          tuples
      }
      ranks = contributions.reduceByKey((x, y) => x + y)
        .mapValues(v => {
          (1 - alpha) + alpha * v
        })
    }
    //Display final pageRanks
    ranks.sortByKey().foreach(println)
  }

  def defaultPartitioner(sc : SparkContext) = {
    val links = sc.parallelize(
      List(
        ("A", List("A", "C", "D")),
        ("B", List("D")),
        ("C", List("B", "D")),
        ("D", List()))
    ).persist()
    var partitioner = links.partitioner
    if (partitioner.isDefined) {
      println("Before sort" + partitioner.get)//print nothing
    }
    val sortLinks = links.sortByKey()
    partitioner = sortLinks.partitioner
    if (partitioner.isDefined) {
      println("After sort" + partitioner.get)//will use rangePartitioner as default
    }
  }

  def intTransformPerElement(x: Int): Int = {
    x * 2
  }

  def intTransformPerPartition(iter: Iterator[Int]): Iterator[Int] = {
    for (elem <- iter) yield elem * 2
  }

  def partitionOp(sc: SparkContext) = {
    val list = sc.parallelize(1 to 10, 3)
    val acc1 = sc.longAccumulator("mapAcc")
    val acc2 = sc.longAccumulator("mapPartitionAcc")
    list.map(x => {
      acc1.add(1)
      intTransformPerElement(x)
    }).collect().foreach(println)

    list.mapPartitions(iter => {
      acc2.add(1)
      intTransformPerPartition(iter)
    }).collect().foreach(println)
    //acc not eq. partitionOperation will take less times
    println("acc1=%d, acc2=%d".format(acc1.value.intValue(), acc2.value.intValue()))
  }

  def main(args: Array[String]): Unit = {
    val time = new DateTime
    val fmt = DateTimeFormat.forPattern("yyyyMMdd hh:mm:ss")

    val appName = "spark-demo-" + time.toString(fmt)
    val master = "local"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

//    hadoopLogAnalysis(sc, args)
//    arrayTest(sc)
//    pageRank(sc)
//    defaultPartitioner(sc)
    partitionOp(sc)
    sc.stop()
  }
}
