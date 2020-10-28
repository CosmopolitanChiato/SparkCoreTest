package com.atguigu.keyvalue2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo_ad_click_top32 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[String] = sc.textFile("input/agent.log")

    // 1. prv-ad,1
    val prvAndAdToOne: RDD[(String, Int)] = dataRDD.map {
      line => {
        val strings = line.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    }

    // 2. prv-ad,sum
    val prvAndAdSum: RDD[(String, Int)] = prvAndAdToOne.reduceByKey(_ + _)

    // 3. prv,(ad, sum)
    val prvAdSum: RDD[(String, (String, Int))] = prvAndAdSum.map {
      case (prvAd, sum) => {
        val ks = prvAd.split("-")
        (ks(0), (ks(1), sum))
      }
    }

    // 4. prv, Iter((ad,sum))
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvAdSum.groupByKey()

    // 5. prv, 排序Iter及取前3
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues {
      datas =>
        datas.toList.sortWith(
          (l, r) => l._2 > r._2
        ).take(3)
    }
    sortRDD.collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
