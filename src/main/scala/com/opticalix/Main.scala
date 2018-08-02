package com.opticalix

import com.github.nscala_time.time.Imports._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.DateTimeFormat

object Main {
  def main(args: Array[String]): Unit = {
    val time = new DateTime
    val fmt = DateTimeFormat.forPattern("yyyyMMdd hh:mm:ss")

    val appName = "spark-demo-" + time.toString(fmt)
    val master = "local"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    arrayTest(sc)
    sc.stop()
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
}
