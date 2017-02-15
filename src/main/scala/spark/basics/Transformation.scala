package spark.basics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jcchoiling on 15/2/2017.
  *
  * map
  * flatMap
  * groupByKey
  * reduceByKey
  * aggregrateByKey
  * sortByKey
  * join
  * cogroup
  * mapPartition
  * mapPartitionWithIndex
  *
  */
object Transformation {

  def main(args: Array[String]): Unit = {

    val sc = sparkContext("Transformation","local[*]")

    joinOps(sc)


    sc.stop()

  }



  def sparkContext(name: String, masterUrl: String): SparkContext = {

    val conf = new SparkConf().setAppName(name).setMaster(masterUrl)
    val sc = new SparkContext(conf)

    sc

  }


  def mapOps(sc: SparkContext) {


  }

  def flatMapOps(sc: SparkContext) {

  }

  def groupByKeyOps(sc: SparkContext) {

  }


  def reduceByKeyOps(sc: SparkContext) {

  }


  def joinOps(sc: SparkContext) {

    val studentName = Array(Tuple2(1,"Spark"), Tuple2(2,"Tachyon"), Tuple2(3,"Hadoop"))
    val studentScore = Array(Tuple2(1, 100), Tuple2(2, 95), Tuple2(3, 65))

    val names = sc.parallelize(studentName)
    val scores = sc.parallelize(studentScore)

    val namesAndScores = names.join(scores) // 重点
    namesAndScores.collect().foreach(println)


  }


}
