package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[String] = sc.textFile("input/agent.log")

    // 2.(prv-ad,1)
    val prvAndAdToOneRDD = dataRDD.map {
      line => {
        val words = line.split(" ")
        (words(1) + "-" + words(4), 1)
      }
    }

    // 3. prv-ad,sum
    val prvAndAdSumRDD: RDD[(String, Int)] = prvAndAdToOneRDD.reduceByKey(_ + _)

    // 4. prv,(ad, sum)
    val prvAdSumRDD: RDD[(String, (String, Int))] = prvAndAdSumRDD.map {
      case (prvAndAd, v) => {
        val prvAdPair: Array[String] = prvAndAd.split("-")
        (prvAdPair(0), (prvAdPair(1), v))
      }
    }
//    prvAdSumRDD.collect().foreach(println)

    // 5. prv,Iterator((ad,sum))
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvAdSumRDD.groupByKey()

    // 6. 对同省的广告降序，取前三
    val take3RDD = groupRDD.mapValues {
      datas => {
        datas.toList.sortWith(
          (l, r) => {
            l._2 > r._2
          }
        ).take(3)
      }
    }
    take3RDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
