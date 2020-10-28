package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require03_PageFlow {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
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

    val ids: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
    val idZipList: List[String] = ids.zip(ids.tail).map {
      case (page1, page2) => {
        page1 + "-" + page2
      }
    }

//    println(idZipList)
//     计算分母
    val idsFenmu: Map[Long, Long] =
      actionRDD
        .filter(action => ids.contains(action.page_id))
        .map(action => (action.page_id, 1L))
        .reduceByKey(_ + _).collect().toMap

    //    idsFenmu.collect().foreach(println)

    // 计算分子
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val pageFlowRDD: RDD[List[String]] = sessionGroupRDD.mapValues(
      datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (l, r) => {
            l.action_time < r.action_time
          }
        )
        val pageidList: List[Long] = actions.map(_.page_id)
        val pageToPageList: List[(Long, Long)] = pageidList.zip(pageidList.tail)
        val pageJumpCounts: List[String] = pageToPageList.map {
          case (page1, page2) => {
            page1 + "-" + page2
          }
        }

        pageJumpCounts.filter(data => idZipList.contains(data))

      }
    ).map(_._2)

    val pageFlowMapRDD: RDD[(String, Long)] = pageFlowRDD.flatMap(list => list).map((_, 1L)).reduceByKey(_ + _)

    pageFlowMapRDD.foreach {
      case (pageflow, sum) => {
        val pageIds: Array[String] = pageflow.split("-")
        val pageIdSum: Long = idsFenmu.getOrElse(pageIds(0).toLong, 1L)
        println(pageflow + "=" + sum.toDouble / pageIdSum)
      }
    }

    //4.关闭连接
    sc.stop()
  }
}
