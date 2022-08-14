package com.tom.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("data/word.txt")
    println(lines.dependencies)
    println("=======================")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("=======================")

    val wordToOne = words.map(
      word => (word, 1)
    )
    println(wordToOne.dependencies)
    println("=======================")

    val wordToCount = wordToOne.reduceByKey(_ + _)
    println(wordToCount.dependencies)
    println("=======================")

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
