package spark.basics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by janicecheng on 17/1/2017.
  */
object RDDBasicOps {
  def main (args: Array[String]) :Unit = {

    val conf = new SparkConf().setAppName("RDDBasicOps").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(1 to 100) // ParallelCollectionRDD
    val results = numbers.map(_ * 2) // MapPartitionsRDD


    results.collect().foreach(print)
    results.take(5)

    val scores = Array(
      Tuple2(1,100),
      Tuple2(2,90),
      Tuple2(2,80),
      Tuple2(2,75),
      Tuple2(3,95),
      Tuple2(4,95),
      Tuple2(5,95),
      Tuple2(2,95),
      Tuple2(1,95)
    )

    val scoresRDD = sc.parallelize(scores)
    val content = scoresRDD.countByKey() //scala.collection.Map[Int,Long] = Map(5 -> 1, 1 -> 2, 2 -> 4, 3 -> 1, 4 -> 1)




    sc.stop()
  }
}