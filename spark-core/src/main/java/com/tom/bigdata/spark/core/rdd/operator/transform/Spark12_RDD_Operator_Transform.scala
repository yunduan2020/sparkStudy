package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(5, 4, 2, 4, 3, 7, 1), 2)

    val newRDD: RDD[Int] = rdd.sortBy(num => num)

    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
