package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object value07_groupby_wordcount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")
    val rdd = sc.makeRDD(strList)
    val wordCount: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).groupBy(word => word).map {
      case (str, strings) => (str, strings.size)
    }
    wordCount.collect().foreach(println)

    rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

    rdd.flatMap(_.split(" ")).groupBy(word=>word).map{
      case (word, words) => (word, words.size)
    }.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
