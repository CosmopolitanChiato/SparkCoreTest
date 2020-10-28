package com.chiato

import org.apache.spark.{SparkConf, SparkContext}

object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD = sc.textFile("input/agent.log")

    //1.prv-ad,1

    //2.prv-ad,sum

    //

    //4.关闭连接
    sc.stop()
  }
}
