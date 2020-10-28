package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object accumulator01_system {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    val sum: LongAccumulator = sc.longAccumulator("sum")

    dataRDD.foreach{
      case (a, v) => {
        sum.add(v)
        println(sum.value)
      }
    }

    println(sum.value)

    //4.关闭连接
    sc.stop()
  }
}
