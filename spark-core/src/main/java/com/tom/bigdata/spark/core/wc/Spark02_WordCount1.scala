package com.tom.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount1 {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark
    // TODO 建立和Spark框架的连接
    // JDBC: Connection
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    // TODO 执行业务操作

    // 1. 读取文件，获取一行一行的数据
    // hello word
    val lines: RDD[String] = sc.textFile("data")

    // 2. 将一行数据进行拆分，形成一个一个得单词（分词）
    // 扁平化： 将整体拆分成个体的操作
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    // Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    // reduceByKey: 相同的key数据，可以对value进行聚合
    // wordToOne.reduceByKey((x, y) => {x + y})
    val wordToCount = wordToOne.reduceByKey(_ + _)

    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }
}

