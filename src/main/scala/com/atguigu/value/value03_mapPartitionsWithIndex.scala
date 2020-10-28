package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object value03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val indexRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
      case (index, datas) => datas.map((index, _))
    }

    indexRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}