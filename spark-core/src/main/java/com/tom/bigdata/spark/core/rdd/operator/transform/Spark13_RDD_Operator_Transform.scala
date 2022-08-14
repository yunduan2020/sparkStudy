package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -双Value类型
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd11 = sc.makeRDD(List("hello", "thank", "hi", "ok"))


    val rdd3: RDD[Int] = rdd1.intersection(rdd2) // 交集
    val rdd4: RDD[Int] = rdd1.union(rdd2) // 并集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2) // 差集
    val rdd6 = rdd1.zip(rdd11) // 拉链
    // 交集，并集和差集要求两个数据源数据类型保持一致
    // 拉链操作两个数据源的类型可以不一致
    // val rdd33: RDD[Int] = rdd11.intersection(rdd2)    // 交集

    println(rdd3.collect().mkString(","))
    println(rdd4.collect().mkString(","))
    println(rdd5.collect().mkString(","))
    println(rdd6.collect().mkString(","))

    sc.stop()
  }
}
