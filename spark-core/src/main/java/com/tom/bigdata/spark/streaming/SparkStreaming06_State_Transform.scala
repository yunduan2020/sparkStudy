package com.tom.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // transform方法可以将底层RDD获取到后进行操作
    // 1.  DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code: Driver端
    val newDS: DStream[String] = lines.transform(
      rdd => {
        // Code：Driver端，（周期性执行）
        rdd.map(
          str => {
            // Code: Executor端
            str
          }
        )
      }
    )
    // Code: Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        // Code: Executor
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
