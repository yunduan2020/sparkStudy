package com.tom.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("NBA", "xxxxx"),
      ("CBA", "xxxxxx"),
      ("WNBA", "xxxxxxx"),
      ("NBA", "xxxxxxxxxxx")
    ))
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /*
  * 自定义分区器
  * 1. 继承Partitioner
  * 2. 重写方法
   */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any):Int = {
      key match {
        case "NBA" => 0
        case "WNBA" => 1
        case _ => 2
      }
    }
  }
}
