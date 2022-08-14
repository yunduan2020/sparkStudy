package com.tom.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._    // 不是导入包， 是把变量中的内容拿过来

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", (name:String) => {
      "Name: " + name
    })

    spark.sql("select age, prefixName(username) from user").show()


    spark.close()
  }
}