package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -Key-Value类型
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("a", 6)
    ))

    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    // join: 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    // 如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    // 如果两个数据源中key中有多个相同的，会依次匹配。可能会出现笛卡尔积，数据量会几何性增长, 导致性能降低
    joinRDD.collect().foreach(println)

    sc.stop()
  }
}
