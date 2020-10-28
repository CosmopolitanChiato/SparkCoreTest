package com.chiato

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object case_Top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[String] = sc.textFile("input/agent.log")

    //1. prv-ad,1
    val prvAndAdToOneRDD = dataRDD.map {
      data => {
        val strings = data.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    }

    //2. prv-ad, sum
    val prvAndAdToSumRDD: RDD[(String, Int)] = prvAndAdToOneRDD.reduceByKey(_ + _)

    //3. prv,(ad, sum)
    val prvAndAdRDD = prvAndAdToSumRDD.map {
      case (prvAndAd, sum) => {
        val prvAd = prvAndAd.split("-")
        (prvAd(0), (prvAd(1), sum))
      }
    }

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvAndAdRDD.groupByKey()
    //4. prv, Iter((ad,sum))

    //5. 排序，取top3
    val top3RDD = groupRDD.mapValues(datas => datas.toList.sortWith(
      (l, r) => l._2 > r._2
    ).take(3))

    top3RDD.collect().foreach(println)
    //
    //4.关闭连接
    sc.stop()
  }
}
