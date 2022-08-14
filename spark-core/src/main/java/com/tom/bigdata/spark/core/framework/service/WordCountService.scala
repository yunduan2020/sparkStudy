package com.tom.bigdata.spark.core.framework.service

import com.tom.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/*
* 服务层
 */
class WordCountService {
  private val wordCountDao = new WordCountDao()

  def dataAnalysis() = {

    val lines: RDD[String] = wordCountDao.readFile("data/word.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    val array: Array[(String, Int)] = wordToSum.collect()
    array
  }
}
