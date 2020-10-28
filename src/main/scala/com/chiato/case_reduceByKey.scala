package com.chiato

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object case_reduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))

    rdd.reduceByKey(_ + _).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
