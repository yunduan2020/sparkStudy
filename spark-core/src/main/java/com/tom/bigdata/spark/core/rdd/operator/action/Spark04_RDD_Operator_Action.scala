package com.tom.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 1, 3, 4), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 1)))

    // TODO - 行动算子
    val intToLong: collection.Map[Int, Long] = rdd1.countByValue()
    println(intToLong)

    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
    println(stringToLong)


    sc.stop()
  }
}
