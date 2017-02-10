package com.sparkDB

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
/**
  * Author he xu 1017925023@qq.com 
  * Date 2017/2/10.
  * http://blog.csdn.net/sundujing/article/details/51480085
  *
  */
object RedisClient {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("wow,my first spark app")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    var jd = new Jedis("172.171.51.154", 6379)
    var str = jd.get("chengshi")
    var strList = str.split(",")
    val a = sc.parallelize(strList, 3)
    val b = a.keyBy(_.length)
    b.collect().foreach(s => println(s._1 + ":" + s._2))

    sc.stop()
  }

}
