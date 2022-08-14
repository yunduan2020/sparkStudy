package com.tom.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 1)))
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    // saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("output2")
    // rdd1.saveAsSequenceFile("output2")


    sc.stop()
  }
}
