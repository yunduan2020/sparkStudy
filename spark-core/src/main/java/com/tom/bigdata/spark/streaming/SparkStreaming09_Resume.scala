package com.tom.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {
  def main(args: Array[String]): Unit = {

    /*
    线程的关闭：
    val thread = new Thread()
    thread.start()

    thread.stop();    // 强制关闭
     */
    val ssc = StreamingContext.getActiveOrCreate("cp", ()=>{
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))

      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

      wordToOne.print()
      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()
    ssc.awaitTermination()    // block阻塞main线程

  }

}
