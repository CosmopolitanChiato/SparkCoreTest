package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object value02_mapPartitions {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    rdd.mapPartitionsWithIndex((ind, datas) => datas.map((ind, _))).collect().foreach(println)

    rdd.mapPartitionsWithIndex{
      case (ind,datas) => datas.map((ind,_))
    }.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
