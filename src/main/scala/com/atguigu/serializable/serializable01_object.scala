package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object serializable01_object {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val user1: User = new User()
    user1.name = "zhangsan"

    val user2: User = new User()
    user2.name = "lisi"

    val userRDD: RDD[User] = sc.makeRDD(List(user1, user2))

    userRDD.foreach(user => println(user.name))

    //4.关闭连接
    sc.stop()
  }
}

class User extends Serializable {
  var name:String = _
}
