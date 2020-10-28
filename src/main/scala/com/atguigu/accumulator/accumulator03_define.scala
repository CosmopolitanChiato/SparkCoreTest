package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object accumulator03_define {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)

    val acc: MyAccumulator = new MyAccumulator()
    sc.register(acc, "acc")

    rdd.foreach{
      case word =>
        acc.add(word)
    }

    println(acc.value)

    //4.关闭连接
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

  var map: mutable.Map[String, Long] = mutable.Map[String, Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if (v.startsWith("H")) {
      map(v) = map.getOrElse(v, 0L) + 1L
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    other.value.foreach {
      case (word, count) => {
        map(word) = map.getOrElse(word, 0L) + count
      }
    }
  }

  override def value: mutable.Map[String, Long] = map
}