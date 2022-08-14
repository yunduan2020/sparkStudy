package com.tom.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
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
    // "hello world" => hello, world
    val word: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 将数据根据单词进行分组，便于统计
    // (hello, hello, hello), (word, word)
    val wordGroup: RDD[(String, Iterable[String])] = word.groupBy(word => word)

    // 4. 对分组后的数据进行转换
    // (hello, hello, hello), (word, word)
    // (hello, 3), (word, 2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }
}
