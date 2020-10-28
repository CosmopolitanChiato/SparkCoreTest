package com.atguigu.keyvalue2

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object KeyValue01_partitionBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)
    val rdd2 = rdd.partitionBy(new HashPartitioner(2))

    rdd2.mapPartitionsWithIndex(
      (index, datas) => datas.map((index, _))
    ).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
