package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object require01_top10Category_method5 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //3.2 将原始数据进行转换
    val actionRDD: RDD[UserVisitAction] = lineRDD.map {
      line => {
        val datas: Array[String] = line.split("_")

        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    }

    val acc: CategoryCountAccumulator = new CategoryCountAccumulator
    sc.register(acc, "acc")

    actionRDD.foreach(action => acc.add(action))

    val accMap: mutable.Map[(String, String), Long] = acc.value

    val group: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)

    val infos: immutable.Iterable[CategoryCountInfo] = group.map {
      case (id, map) => {
        val click: Long = map.getOrElse((id, "click"), 0L)
        val order: Long = map.getOrElse((id, "order"), 0L)
        val pay: Long = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }

    infos.toList.sortWith(
      (l,r) => {
        if(l.clickCount>r.clickCount) {
          true
        } else {
          false
        }
      }
    ).take(10).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

  var map = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if (action.click_category_id != -1) {
      val key: (String, String) = (action.click_category_id.toString, "click")
      map(key) = map.getOrElse(key, 0L) + 1L
    } else if (action.order_category_ids != "null") {
      val ids: Array[String] = action.order_category_ids.split(",")
      for (id <- ids) {
        val key: (String, String) = (id, "order")
        map(key) = map.getOrElse(key, 0L) + 1L
      }

    } else if (action.pay_category_ids != "null") {
      val ids: Array[String] = action.pay_category_ids.split(",")
      for (id <- ids) {
        val key: (String, String) = (id, "pay")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach {
      case (key, count) => {
        map(key) = map.getOrElse(key, 0L) + count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}