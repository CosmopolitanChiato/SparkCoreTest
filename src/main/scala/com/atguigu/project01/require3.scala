package com.atguigu.project01

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require3 {

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

    val ids = List(1, 2, 3, 4, 5, 6, 7)
    val idsZip: List[String] = ids.zip(ids.tail).map {
      case (data1, data2) => (data1 + "-" + data2)
    }

    val b_ids: Broadcast[List[Int]] = sc.broadcast(ids)

    val idsSum: Map[Long, Long] =
      actionRDD.filter(act => b_ids
        .value.contains(act.page_id))
        .map(act => (act.page_id, 1L))
        .reduceByKey(_ + _).collect().toMap

    //    idsSum.foreach(println)
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val pageFlow: RDD[List[String]] = sessionGroupRDD.mapValues(
      datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (l, r) => l.action_time < r.action_time
        )

        val pageidList: List[Long] = actions.map(_.page_id)
        val pageJump: List[String] = pageidList.zip(pageidList.tail).map {
          case (page1, page2) => {
            page1 + "-" + page2
          }
        }
        pageJump.filter(data => idsZip.contains(data))
      }
    ).map(_._2)

    //  RDD[List[String]]
    val pageFlowCount: RDD[(String, Long)] = pageFlow.flatMap(list => list).map((_, 1L)).reduceByKey(_ + _)

    // RDD[(String, Long)]
    //  idsSum: Map[Long, Long]
    pageFlowCount.foreach{
      case (pageAndPage, pageCounts) =>{
        val pages: Array[String] = pageAndPage.split("-")
        val sumCounts: Long = idsSum.getOrElse(pages(0).toLong, 1L)
        println(pageAndPage + ":" + pageCounts.toDouble/sumCounts)
      }
    }

    //4.关闭连接
    sc.stop()
  }
}
