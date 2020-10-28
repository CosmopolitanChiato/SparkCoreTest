package com.atguigu.keyvalue2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[String] = sc.textFile("input/agent.log")
    val prvAndAdvToOneRDD = dataRDD.map {
      line => {
        val strings = line.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    }
    // prv-adv,sum
    val prvAndAdvToSumRDD: RDD[(String, Int)] = prvAndAdvToOneRDD.reduceByKey(_ + _)

    //prv,(adv,sum)
    val prvAdvAndSumRDD: RDD[(String, (String, Int))] = prvAndAdvToSumRDD.map {
      case (prvAndAdv, sum) => {
        val ks: Array[String] = prvAndAdv.split("-")
        (ks(0), (ks(1), sum))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvAdvAndSumRDD.groupByKey()

    val sortRDD = groupRDD.mapValues {
      datas =>
        datas.toList.sortWith(
          (l, r) => l._2 > r._2
        ).take(3)
    }

    sortRDD.collect().foreach(println)

    // 2.prv-adv,1 => prv-adv, sum


    //4.关闭连接
    sc.stop()
  }
}
