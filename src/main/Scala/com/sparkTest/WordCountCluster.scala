package com.sparkTest

/**
  * Author he xu 1017925023@qq.com 
  * Date 2017/2/10.
  */
import org.apache.spark.{SparkConf, SparkContext}
object WordCountCluster {
  def main(args: Array[String]){
    /**
      * 第一步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要连接的Spark集群的Master的URL，
      * 如果设置为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差
      * （例如只有1G的内存）的初学者
      */
    val conf =new SparkConf()//创建SparkConf对象，由于全局只有一个SparkConf所以不需要工厂方法
    conf.setAppName("wow,my first spark app")//设置应用程序的名称，在程序的监控界面可以看得到名称
    // conf.setMaster("spark://Master:7077")//此时程序在Spark集群
    /**
      * 第二步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须要有一个
      * SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler，TaskScheduler，SchedulerBacked，
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc=new SparkContext(conf)//创建SpackContext对象，通过传入SparkConf实例来定制Spark运行的具体参数的配置信息
    /**
      * 第三步：根据具体的数据来源（HDFS，HBase，Local，FileSystem，DB，S3）通过SparkContext来创建RDD
      * RDD的创建基本有三种方式，（1）根据外部的数据来源（例如HDFS）（2）根据Scala集合（3）由其它的RDD操作
      * 数据会被RDD划分为成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */
    //读取HDFS文件并切分成不同的Partition

    val lines=sc.textFile("hdfs://node1:8020/tmp/harryport.txt")
    //val lines=sc.textFile("/index.html")
    //类型推断 ,也可以写下面方式
    //   val lines : RDD[String] =sc.textFile("D://spark-1.6.1-bin-hadoop2.6//README.md", 1)
    /**
      * 第四步：对初始的RDD进行Transformation级别的处理，例如map，filter等高阶函数
      * 编程。来进行具体的数据计算
      * 第4.1步：将每一行的字符串拆分成单个的单词
      */
    //对每一行的字符串进行单词拆分并把所有行的结果通过flat合并成一个大的集合
    val words = lines.flatMap { line => line.split(" ") }
    /**
      * 第4.2步在单词拆分的基础上，对每个单词实例计数为1，也就是word=>（word,1）tuple
      */
    val pairs = words.map { word => (word,1) }
    /**
      * 第4.3步在每个单词实例计数为1的基础之上统计每个单词在文中出现的总次数
      */
    //对相同的key进行value的累加（包括local和Reduce级别的同时Reduce）
    val wordCounts = pairs.reduceByKey(_+_)
    //打印结果
    wordCounts.collect.foreach(wordNumberPair => println(wordNumberPair._1 + ":" +wordNumberPair._2))
    //释放资源
    sc.stop()
  }
}

/**
  * 本地模式 http://blog.csdn.net/sundujing/article/details/51333525#comments
  */
object WordCount {
  def main(args: Array[String]){
    /**
      * 第一步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要连接的Spark集群的Master的URL，
      * 如果设置为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差
      * （例如只有1G的内存）的初学者
      */
    val conf =new SparkConf()//创建SparkConf对象，由于全局只有一个SparkConf所以不需要工厂方法
    conf.setAppName("wow,my first spark app")//设置应用程序的名称，在程序的监控界面可以看得到名称
    conf.setMaster("local")//此时程序在本地运行，不需要安装Spark集群
    /**
      * 第二步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须要有一个
      * SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler，TaskScheduler，SchedulerBacked，
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc=new SparkContext(conf)//创建SpackContext对象，通过传入SparkConf实例来定制Spark运行的具体参数的配置信息
    /**
      * 第三步：根据具体的数据来源（HDFS，HBase，Local，FileSystem，DB，S3）通过SparkContext来创建RDD
      * RDD的创建基本有三种方式，（1）根据外部的数据来源（例如HDFS）（2）根据Scala集合（3）由其它的RDD操作
      * 数据会被RDD划分为成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */
    //读取本地文件并设置为一个Partition
    val lines=sc.textFile("D://spark-1.6.1-bin-hadoop2.6//README.md", 1)//第一个参数为为本地文件路径，第二个参数minPartitions为最小并行度，这里设为1
    //类型推断 ,也可以写下面方式
    //   val lines : RDD[String] =sc.textFile("D://spark-1.6.1-bin-hadoop2.6//README.md", 1)
    /**
      * 第四步：对初始的RDD进行Transformation级别的处理，例如map，filter等高阶函数
      * 编程。来进行具体的数据计算
      * 第4.1步：将每一行的字符串拆分成单个的单词
      */
    //对每一行的字符串进行单词拆分并把所有行的结果通过flat合并成一个大的集合
    val words = lines.flatMap { line => line.split(" ") }
    /**
      * 第4.2步在单词拆分的基础上，对每个单词实例计数为1，也就是word=>（word,1）tuple
      */
    val pairs = words.map { word => (word,1) }
    /**
      * 第4.3步在每个单词实例计数为1的基础之上统计每个单词在文中出现的总次数
      */
    //对相同的key进行value的累加（包括local和Reduce级别的同时Reduce）
    val wordCounts = pairs.reduceByKey(_+_)
    //打印结果
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + ":" +wordNumberPair._2))
    //释放资源
    sc.stop()
  }

}
