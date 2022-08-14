package com.tom.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    // val sparkConf = new SparkConf().setMaster("spark://192.168.10.102:7077").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // TODO 算子 -map
    // 1,2,3,4
    // 2,4,6,8

    // 转换函数
    def mapFunction(num: Int): Int = {
      num * 2
    }

    // val mapRDD: RDD[Int] = rdd.map(mapFunction)
    // val mapRDD: RDD[Int] = rdd.map((num:Int)=>{num*2})
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
