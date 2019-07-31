package com.opticalix

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.{DenseVector, SQLDataTypes}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, _}

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
//    val df = changeColumnsType(spark.createDataFrame(rowRdd, dfSchema), Array("households", "housingMedianAge",
//      "latitude", "longitude", "medianHouseValue", "medianIncome",
//      "population", "totalBedRooms", "totalRooms"), DoubleType)
    var df = changeColumnsType(spark.createDataFrame(rowRdd, dfSchema), Array(
      "longitude", "latitude",//经纬度
      "housingMedianAge",//房龄
      "totalRooms", "totalBedRooms",//房屋数信息
      "population", //区域人口
      "households", //区域家庭
      "medianIncome", //区域收入
      "medianHouseValue" //区域房价
    ), DoubleType)
    df.printSchema()
//    df.show(5)

    //todo udf https://dongkelun.com/2018/08/02/sparkUDF/
//    df.groupBy("housingMedianAge").count().orderBy(df("housingMedianAge").asc).show()

    //exclude empty
    df.na.drop()

    //refine columns, features selection
    df = df
      .withColumn("medianHouseValue", col("medianHouseValue") / 10000)
      .withColumn("roomsPerHousehold", col("totalRooms") / col("households"))
      .withColumn("populationPerHousehold", col("population") / col("households"))
      .withColumn("bedroomsPerRoom", col("totalBedRooms") / col("totalRooms"))
      .withColumn("bedroomsPerHousehold", col("totalBedRooms") / col("households"))
    df = df.select("medianHouseValue",
      "totalRooms",
      "totalBedRooms",
      "population",
      "households",
      "medianIncome",
      "roomsPerHousehold",
      "populationPerHousehold",
      "bedroomsPerRoom",
      "bedroomsPerHousehold")
    df.show(5)

    //prepare label and features
    val input = df.rdd.map(x => {
      val seq = x.toSeq.slice(1, x.size)
      val array = seq.map(_.toString.toDouble).toArray
      Row.fromTuple((x.toSeq.head, new DenseVector(array)))
    })
    val structType = StructType(Array(StructField("label", DoubleType), StructField("features", SQLDataTypes.VectorType)))
    df = spark.createDataFrame(input, structType)
    df.show(5)

    //scale
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("features_scaled").fit(df)
    val scaledDf = scaler.transform(df)
    scaledDf.show(5)

    //ml. split dataSet and grid search
    val Array(train, test) = scaledDf.randomSplit(Array(0.8d, 0.2d), seed = 12345)
    val lr = new LinearRegression().setFeaturesCol("features_scaled").setLabelCol("label")
      .setMaxIter(20)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)

//    val lrModel = lr.fit(train)
//    val predicted = lrModel.transform(test)
//    val predition = predicted.select("prediction").rdd.map(_(0))
//    val labels = predicted.select("label").rdd.map(_(0))
//    predition.zip(labels).take(20).foreach(println)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(1, 0.1, 0.01, 0.001))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    val model = trainValidationSplit.fit(train)
    model.transform(test)
      .select("features", "label", "prediction")
      .show()
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
