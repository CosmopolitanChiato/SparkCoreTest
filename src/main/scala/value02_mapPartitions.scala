import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object value02_mapPartitions {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 4)
    val coalesceRdd: RDD[Int] = rdd.coalesce(2)
    coalesceRdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        datas
      }).collect()

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4, 1)
    rdd1.glom().map(_.max).collect().foreach(println)
    //    val listRDD=sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)), 2)
    //    listRDD.flatMap(list =>list).collect().foreach(println)

    val rdd2: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    rdd2.flatMap(_.split(" ")).groupBy(word => word).map(elem => (elem._1, elem._2.size)).collect().foreach(println)

    rdd2.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    //    rdd.mapPartitions(itr => itr.map(_*2)).collect().foreach(println)
    //    val rdd2: RDD[String] = sc.textFile("input")
    //    rdd2.collect().foreach(println)
    //    sc.textFile("input")
    //      .flatMap(_.split(" "))
    //      .map((_, 1)).reduceByKey(_ + _)
    //      .collect().foreach(println)


    val distinctRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))
    distinctRdd.distinct().collect().foreach(println)

    val rdd3: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    rdd3.coalesce(2).mapPartitionsWithIndex((index, items) => items.map((index, _))).collect().foreach(println)

    rdd3.coalesce(2)
      .mapPartitionsWithIndex((index, items) => {
        for (elem <- items) {println(index + "=>" + elem)}
        items
      }).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
