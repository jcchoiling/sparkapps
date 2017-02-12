package spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by jcchoiling on 8/2/2017.
  *
  * 通过 SparkSQL 的方式查询什么年齡和姓别人仕对电影的爱好
  *
  */
object MovieAnalysisSparkSQLOps {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MovieAnalysisSparkSQLOps")
      .master("local")
      .config("spark.sql.warehouse","spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()



  }
}
