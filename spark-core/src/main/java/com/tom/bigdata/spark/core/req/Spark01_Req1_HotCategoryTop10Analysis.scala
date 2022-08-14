package com.tom.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    // TODO:Top10热门品类
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类ID， 点击数量）
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data = action.split("_")
        data(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val data = action.split("_")
        (data(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类的下单数量：（品类ID， 下单数量）
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data = action.split("_")
        data(8) != "null"
      }
    )

    // orderid => 1,2,3
    // [(1,1), (2,1), (3,1)]
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val data = action.split("_")
        val cid = data(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4. 统计品类的支付数量：（品类ID， 支付数量）
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data = action.split("_")
        data(10) != "null"
      }
    )

    // orderid => 1,2,3
    // [(1,1), (2,1), (3,1)]
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val data = action.split("_")
        val cid = data(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5. 将品类进行排序，并且取前10名
    //   点击数量相同，下单数量排序，支付数量排序
    //   元组排序：先比较第一个，再比较第二个，依次类推
    //   （品类ID， （点击数量, 下单数量， 支付数量））
    // cogroup = connect + group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
        clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }
}
