package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object value11_coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)

    rdd.mapPartitionsWithIndex{
      case (index, datas) => datas.map((index, _))
    }.collect().foreach(println)

    coalesceRDD.mapPartitionsWithIndex{
      case (index, datas) => datas.map((index, _))
    }.collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
