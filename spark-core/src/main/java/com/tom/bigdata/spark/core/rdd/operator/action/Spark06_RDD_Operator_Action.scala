package com.tom.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子

    // foreach 其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("=======================")
    // foreach其实是Executor端内存数据打印
    rdd.foreach(println)

    // 算子：Operator(操作)
    //       RDD方法和Scala集合对象的方法不一样
    //       集合对象的方法是在同一个节点的内存中完成的
    //       RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    //       为了区分不同的处理效果，所以将RDD的方法称之为算子
    //       RDD的方法外部操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行


    sc.stop()
  }
}
