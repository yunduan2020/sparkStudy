package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("output")
    // [1, 2], [3, 4]


    val mapRDD = rdd.map(_ * 2)
    mapRDD.saveAsTextFile("output1")
    // [2, 4], [6, 8]

    sc.stop()
  }
}
