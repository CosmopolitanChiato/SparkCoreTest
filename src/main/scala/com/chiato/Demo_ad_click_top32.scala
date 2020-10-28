package com.chiato

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo_ad_click_top32 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[String] = sc.textFile("input/agent.log")

    val prvAndAdOneRDD: RDD[(String, Int)] = dataRDD.map(data => {
      val fields: Array[String] = data.split(" ")
      (fields(1) + "-" + fields(4), 1)
    })
    val prvAndAdSumRDD: RDD[(String, Int)] = prvAndAdOneRDD.reduceByKey(_ + _)
    val prvAdSumRDD: RDD[(String, (String, Int))] = prvAndAdSumRDD.map {
      case (prvAndAd, v) => {
        val prvAd: Array[String] = prvAndAd.split("-")
        (prvAd(0), (prvAd(1), v))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvAdSumRDD.groupByKey()
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => datas.toList.sortWith(
        (l, r) => l._2 > r._2
      ).take(3)
    )
    sortRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
