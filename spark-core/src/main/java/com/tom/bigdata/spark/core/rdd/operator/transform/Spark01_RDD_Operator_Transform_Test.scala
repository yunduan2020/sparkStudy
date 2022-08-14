package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("spark://hadoop102:7077").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -map
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    // 长的字符串
    // 短的字符串
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val data = line.split(" ")
        data(6)
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
