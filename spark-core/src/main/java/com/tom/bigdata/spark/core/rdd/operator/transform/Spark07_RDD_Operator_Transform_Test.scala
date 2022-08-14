package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -filter
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    rdd.filter(
      line => {
        val data = line.split(" ")
        val time = data(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)

    sc.stop()
  }
}
