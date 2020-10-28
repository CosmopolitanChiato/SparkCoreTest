package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object action07_aggregate {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)

    println(rdd.aggregate(0)(_ + _, _ + _))
    println(rdd.aggregate(10)(_ + _, _ + _))

    println(rdd.fold(0)(_ + _))
    println(rdd.fold(10)(_ + _))

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val res: collection.Map[Int, Long] = rdd1.countByKey()
    println(res)

    //4.关闭连接
    sc.stop()
  }
}
