package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -flatmap
    var rdd : RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))
    val flatRDD: RDD[Int] = rdd.flatMap(
      list => {    // 表示传入的是list集合
        list        // 表示将返回的结果用list封装
      }
    )
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
