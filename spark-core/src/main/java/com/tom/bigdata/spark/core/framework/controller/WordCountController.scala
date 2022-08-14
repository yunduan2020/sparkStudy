package com.tom.bigdata.spark.core.framework.controller

import com.tom.bigdata.spark.core.framework.service.WordCountService

/*
* 控制层
 */
class WordCountController {

  private val wordCountService = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    // TODO 执行业务操作
    val array: Array[(String, Int)] = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
