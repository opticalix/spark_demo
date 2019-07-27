package com.opticalix

import java.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, sql}
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Main {

  /**
    * call withColumn(), change data type of column
    * @param df
    * @param names
    * @param newType
    * @return dataFrame
    */
  def changeColumnsType(df: DataFrame, names: Array[String], newType: DataType) = {
    var ret: DataFrame = df
    for (name <- names) {
      ret = ret.withColumn(name, df(name).cast(newType))
    }
    ret
  }

  def californiaHouse(spark: SparkSession) = {
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    import sqlc.implicits._
    val calDataPath = "res/cal_housing.data"
    val calDataDomain = "res/cal_housing.domain"

    //read
    val dataRdd = sc.textFile(calDataPath)
    val domainRdd = sc.textFile(calDataDomain)
    domainRdd.collect().foreach(println)
    dataRdd.take(1).foreach(println)

    //build dataFrame
    val rowRdd = dataRdd.map(l => l.split(",")).map(arr => Row.fromSeq(arr))
    val colNames = domainRdd.map(desc => desc.substring(0, desc.indexOf(":"))).map(name => StructField(name, StringType, true)).collect()
    val dfSchema = StructType(colNames)
    val df = changeColumnsType(spark.createDataFrame(rowRdd, dfSchema), Array("households", "housingMedianAge", "latitude", "longitude", "medianHouseValue", "medianIncome", "population", "totalBedRooms", "totalRooms"), DoubleType)
    df.printSchema()

    //analysis
    df.groupBy("housingMedianAge").count().orderBy(df("housingMedianAge").asc).show()
  }

  def main(args: Array[String]): Unit = {
    val appName = "California House"
    val master = "local"
    val conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.executor.memory", "512mb")
    val spark = new SparkSession.Builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    californiaHouse(spark)

    sc.stop()
  }
}
