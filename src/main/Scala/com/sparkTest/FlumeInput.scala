package com.sparkTest

import org.apache.spark._
import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume._
/**
  * Author he xu 1017925023@qq.com 
  * Date 2017/2/10.
  * 官网：http://spark.apache.org/docs/1.6.1/streaming-programming-guide.html
  * Creating StreamingContext with SparkConf instead of SparkContext
  */
object FlumeInputOfficial{
  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent from a starvation scenario.
  val conf = new SparkConf().setMaster("node1").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1)) //底层处理新数据的批次间隔

}

/**
  * github上的  spark1.3
  */
object FlumeInput {
  def main(args: Array[String]) {
    val receiverHostname = args(0)
    val receiverPort = args(1).toInt
    val conf = new SparkConf().setAppName("FlumeInput")
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))
    println(s"Creating flume stream on $receiverHostname $receiverPort")
    val events = FlumeUtils.createStream(ssc, receiverHostname, receiverPort)
    // Assuming that our flume events are UTF-8 log lines
    val lines = events.map{e => new String(e.event.getBody().array(), "UTF-8")}
    println("Starting StreamingContext")
    lines.print()
    // start our streaming context and wait for it to "finish"
    ssc.start()
    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination(10000)
    ssc.stop()
    println("Done")
  }
}
