package basics

import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by jcchoiling on 21/1/2017.
  *   "Scala" -> 13L,
      "Hadoop" -> 11L,
      "Java" -> 23L,
      "Kafka" -> 10L
  */
object MapPartitionsOps_scala {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MapPartitionsOps")
      .master("local")
      .getOrCreate()

    val names = spark.sparkContext.parallelize(Array("Spark", "Scala", "Hadoop", "Java", "Kafka"),3)

    val table = Map(
      "Spark" -> 6L,
      "Scala" -> 13L,
      "Hadoop" -> 11L,
      "Java" -> 23L,
      "Kafka" -> 10L
    )

    names.mapPartitions(partition => {

      val ages = ArrayBuffer[Long]()

      while (partition.hasNext){
        val age = partition.next()
        ages.append(table(age))
      }

      ages.iterator
    }).collect().foreach(println)


    names.mapPartitionsWithIndex((partitionIndex,partition) => {

      val indexed = ArrayBuffer[String]()

      while (partition.hasNext){
        val name = partition.next()
        indexed.append(partitionIndex + "_" +name)
      }

      indexed.iterator
    }).collect().foreach(println)

    spark.stop()


  }
}
