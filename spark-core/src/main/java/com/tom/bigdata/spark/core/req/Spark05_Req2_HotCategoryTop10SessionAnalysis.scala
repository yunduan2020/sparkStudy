package com.tom.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {

    // TODO:Top10热门品类
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparConf)

    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    val top10Ids: Array[String] = top10Category(actionRDD)

    // 1. 过滤原始数据，保留点击前10品类ID
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data: Array[String] = action.split("_")
        if (data(6) != "-1") {
          top10Ids.contains(data(6))
        } else {
          false
        }
      }
    )

    // 2. 根据品类ID和sessionId进行点击量的统计
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val data: Array[String] = action.split("_")
        ((data(6), data(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 将统计的结果进行结构的转换
    // ((品类ID， sessionId), sum) => (品类ID， （sessionId， sum））
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    // 4. 相同的品类进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 5. 将分组后的数据进行点击量的排序，取前10名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    resultRDD.collect().foreach(println)

    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]) = {
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val data = action.split("_")
        if (data(6) != "-1") {
          // 点击的场合
          List((data(6), (1, 0, 0)))
        } else if (data(8) != "null") {
          // 下单的场合
          val ids = data(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (data(10) != "null") {
          // 支付的场合
          val ids = data(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    analysisRDD.sortBy(_._2, false).take(10).map(_._1)

  }
}
