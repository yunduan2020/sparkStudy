package com.tom.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()    // 不是导入包， 是把变量中的内容拿过来
    import spark.implicits._

    // 读取MySQL数据
    /// spark.read

    // TODO 关闭环境
    spark.close()
  }
}