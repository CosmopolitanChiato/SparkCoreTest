import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoreTest {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)
    rdd.mapPartitionsWithIndex((index, datas) => datas.map((index, _))).collect().foreach(println)

    rdd.glom().map(_.max).collect().foreach(println)

    rdd.groupBy(_ % 2).collect().foreach(println)

    rdd.filter(_ % 2 == 0).collect().foreach(println)

    val rdd2: RDD[Int] = sc.makeRDD(1 to 10, 2)
    rdd2.sample(false, 0.5).collect().foreach(println)

    val distinctRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))
    distinctRdd.distinct().collect().foreach(println)


    val strings: List[String] = List("Hello Spark", "Hello Scala")
    val rdd3: RDD[String] = sc.makeRDD(strings)
    rdd3.flatMap(_.split(" ")).groupBy(word => word).map(item => (item._1, item._2.size)).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
