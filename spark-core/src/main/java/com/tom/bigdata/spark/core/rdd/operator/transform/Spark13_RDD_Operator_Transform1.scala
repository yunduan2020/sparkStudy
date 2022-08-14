package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -双Value类型
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // Can only zip RDDs with same number of elements in each partition
    // 两个数据源要求分区数量保持一致，两个数据源要求分区中数据数量保持一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6),2)

    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)

    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
