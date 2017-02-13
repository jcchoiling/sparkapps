package spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月1日；
  * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
  *   数据采集：企业中一般越来越多的喜欢直接把Server中的数据发送给Kafka，因为更加具备实时性；
  *   数据过滤：趋势是直接在Server端进行数据过滤和格式化，当然采用Spark SQL进行数据的过滤也是一种主要形式；
  *   数据处理：
  *     1，一个基本的技巧是，先使用传统的SQL去实现一个下数据处理的业务逻辑（自己可以手动模拟一些数据）；
  *     2，再一次推荐使用DataSet去实现业务功能尤其是统计分析功能；
  *     3，如果你想成为专家级别的顶级Spark人才，请使用RDD实现业务功能，为什么？运行的时候是基于RDD的！
  *
  *  数据：强烈建议大家使用Parquet
  *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
  *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
  *  3，"movies.dat"：MovieID::Title::Genres
  *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
  */

object EB_MovieUserAnalysisDataSet {

  case class User(UserID: String, Gender: String, Age: String, OccupationID: String, Zip_Code: String)
  case class Ratings(UserID: String, MovieID: String, Rating: Double, Timestamp: String)
  case class Movies(MovieID: String, Title: String, Genres: String)
  case class Occupations(OccupationID: String, OccupationName: String)


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    var masterUrl = "local[1]"  //默认程序运行在本地Local模式中，主要是学习和测试
    var dataPath = "src/main/resources/general/" //数据存放的目录

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("EB_MovieUserAnalysisDataSet")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val usersInfo = spark.read.parquet(dataPath + "/users_eb.parquet")
    val usersAccessLog = spark.read.parquet(dataPath + "/log_eb.parquet")

    usersInfo.select(count($"UserID")).show() //SELECT count(*) FROM usersInfo
    usersAccessLog.select(count($"UserID")).show() //SELECT count(*) FROM usersAccessLog

//    usersInfo.select("name", "registeredTime", "userID").write.save("users_eb.parquet")
//    usersAccessLog.select("consumed", "logID", "time", "typed","userID").write.save("log_eb.parquet")


    /**
      * filter 之后进行 join, 然后进行 groupBy，groupBy 之后进行 agg，最后 sort
      */
//    usersAccessLog.filter("time >= 2016-10-01 and time <= 2016-10-10")
//      .join(usersInfo, usersAccessLog("UserID") === usersInfo("UserID"))
//      .groupBy(usersInfo("UserID"), usersInfo("UserName"))
//      .agg(count(usersAccessLog("time")).alias("userCount"))
//      .sort($"userCount".desc)
//      .limit(10)
//      .show()








    spark.stop()




  }
}
