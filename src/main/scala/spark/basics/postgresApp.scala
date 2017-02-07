package spark.basics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by jcchoiling on 7/2/2017.
  */
object postgresApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("postgresApp")
      .master("local")
      .config("spark.sql.warehouse","spark-warehouse")
      .getOrCreate()

    val jdbcDF = spark
      .read.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/ott")
      .option("dbtable", "person")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    jdbcDF.show()

  }
}
