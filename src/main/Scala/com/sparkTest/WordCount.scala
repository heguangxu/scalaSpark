package com.sparkTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author he xu 1017925023@qq.com 
  * Date 2017/1/23.
  */
/**
  * 实现a，b的出现次数查询
  */
object WordCount {
  def main(args: Array[String]) {
    val logFile = "D:/text.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

/**https://github.com/databricks/learning-spark/tree/master/src/main/scala/com/oreilly/learningsparkexamples/scala
  *Simplify the mini complete example and exit 0 and end of ch06 example…
  */
object WordCount2 {
  def main(args: Array[String]) {  //直接run的情况下，args为空。
    //返回sparkConf的master配置，指明本地还是在yanrn上跑的
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    //初始化sparkContext
    val conf = new SparkConf().setMaster(master).setAppName("WordCount")
    val sc = new SparkContext(conf)
    //返回输入的内容
    val input = args.length match {
      case x: Int if x > 1 => sc.textFile(args(1))

      /**
        * 构建一个RDD----ParallelCollectionRDD
        */
      //parallelize制作RDD的，是ParallelCollectionRDD，创建一个并行集合。
      case _ => sc.parallelize(List("pandas", "i like pandas")) //输入的内容是个列表，内存中不存在,
    }
    //匹配规则，映射结果，切分成单词
    val words = input.flatMap(line => line.split(" "))
//    返回最终结果
    args.length match {
      case x: Int if x > 2 => {
        val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
        //结果保存成文件（args=[style,inputPath,outPath]）
        counts.saveAsTextFile(args(2))
      }
      case _ => {
        val wc = words.countByValue()
        //结果直接打印
        println(wc.mkString(","))
      }
    }
  }
}

/**
  * http://blog.csdn.net/li385805776/article/details/19683533
  * 第一个Spark On Yarn程序
  */
object MyWordCount {
  def main(args: Array[String]) {
//本地版本
    val conf = new SparkConf().setMaster("local").setAppName("my word count")
//    val spark = new SparkContext("local","my word count",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    val sc = new SparkContext(conf)
    val file = sc.textFile("data/input/abc.txt")
    val counts = file.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
    counts.saveAsTextFile("data/out")
    sc.stop()

    /**
      *     HDFS版本
      */
/*  val spark = new SparkContext("yarn-standalone","my word count",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))

    val file = spark.textFile("hdfs://127.0.0.1:9000/user/jack/input")
    val wordcounts = file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    wordcounts.saveAsTextFile("hdfs://127.0.0.1:9000/user/jack/myWordCountOutput")
    spark.stop()*/
  }
}

/**
  * 步骤详解
  */
object stepDetail{
  def main(args: Array[String]): Unit = {
    val inputFile = "data/input/input.txt"
    val outputFile = "data/out/out.txt"
    /**
      * init spark 初始化sparkContext1
      */
    val conf = new SparkConf().setMaster("local").setAppName("my APP")
    //本地local运行，应用名称为my APP
    val sc = new SparkContext(conf)
    /** 使用方法来进行操作（比如用文本文件创建RDD并操控它们）
      *
      * 读取我们的输入数据
      */
    val input = sc.textFile(inputFile)
    //切分成一个个单词
    val words = input.flatMap(line => line.split(" "))
    //转换为键值对并计数
    val counts = words.map(word =>(word,1)).reduceByKey{case(x,y) => x+y}
    //将统计出的单词总数存入一个文本文件，引发求值
    counts.saveAsTextFile(outputFile)
    /**
      * 最后，关闭spark可以调用SParkContext的stop方法
      */
    sc.stop()

  }
}