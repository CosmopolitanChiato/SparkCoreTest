package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayOps, ListBuffer}

object require01_top10Category_method3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val actRDD: RDD[UserVisitAction] = lineRDD.map(
      line => {
        val infos: Array[String] = line.split("_")
        UserVisitAction(
          infos(0),
          infos(1).toLong,
          infos(2),
          infos(3).toLong,
          infos(4),
          infos(5),
          infos(6).toLong,
          infos(7).toLong,
          infos(8),
          infos(9),
          infos(10),
          infos(11),
          infos(12).toLong
        )
      }
    )

    // RDD[UserVisitAction]
    val infoRDD: RDD[CategoryCountInfo] = actRDD.flatMap(
      act => {
        if (act.click_category_id != -1) {
          List(CategoryCountInfo(act.click_category_id.toString, 1, 0, 0))
        } else if (act.order_category_ids != "null") {
          val ids: ArrayOps.ofRef[String] = act.order_category_ids.split(",")

          val list: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 1, 0))
          }

          list
        } else if (act.pay_category_ids != "null") {
          val ids: Array[String] = act.pay_category_ids.split(",")

          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]()
          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 0, 1))
          }
          list
        } else {
          Nil
        }

      }
    )

    // RDD[CategoryCountInfo]
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(info => info.categoryId)

    val mapRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(
      datas => datas.reduce(
        (info1, info2) => {
          info1.clickCount += info2.clickCount
          info1.orderCount += info2.orderCount
          info1.payCount += info2.payCount
          info1
        }
      )
    ).map(_._2)

    val sortRDD: RDD[CategoryCountInfo] = mapRDD.sortBy(
      info => (info.clickCount, info.orderCount, info.payCount),false)
    sortRDD.take(10).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
