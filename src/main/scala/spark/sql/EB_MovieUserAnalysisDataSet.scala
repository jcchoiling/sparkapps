package spark.sql

import sys.process._

import jdk.nashorn.internal.runtime.UserAccessorProperty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月21日；
  * 电商用户行为分析系统：在实际的生产环境下一般都是J2EE+Hadoop+Spark+DB的方式实现的综合技术栈，在使用Spark进行电商用户行为分析的
  * 时候一般都都会是交互式的，什么是交互式的？也就是说公司内部人员例如营销部门人员向按照特定时间查询访问次数最多的用户或者购买金额最大的
  * 用户TopN,这些分析结果对于公司的决策、产品研发和营销都是至关重要的，而且很多时候是立即向要结果的，如果此时使用Hive去实现的话，可能会
  * 非常缓慢（例如1个小时），而在电商类企业中经过深度调优后的Spark一般都会比Hive快5倍以上，此时的运行时间可能就是分钟级别，这个时候就可以
  * 达到即查即用的目的，也就是所谓的交互式，而交互式的大数据系统是未来的主流！
  *
  * 我们在这里是分析电商用户的多维度的行为特征，例如分析特定时间段访问人数的TopN、特定时间段购买金额排名的TopN、注册后一周内购买金额排名TopN、
  * 注册后一周内访问次数排名Top等，但是这里的技术和业务场景同样适合于门户网站例如网易、新浪等，也同样适合于在线教育系统，例如分析在线教育系统的学员
  * 的行为，当然也适用于SNS社交网络系统，例如对于婚恋网，我们可以通过这几节课讲的内容来分析最匹配的Couple，再例如我们可以分析每周婚恋网站访问
  * 次数TopN,这个时候就可以分析出迫切想找到对象的人，婚恋网站可以基于这些分析结果进行更精准和更有效（更挣钱）的服务！
  *
  */

object EB_MovieUserAnalysisDataSet {


  case class UserLog(logID: Long, UserID: Long, time: String, typed: Long,  consumed: Double)

  case class LogOnce(logID: Long, UserID: Long, count: Long)
  case class ConsumeOnce(logID: Long, UserID: Long, consumed: Double)


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

    /**
      * Read data from the source Json format and save it in the Parquet format
      * users.json: {"userID": 0, "name": "spark0", "registeredTime": "2016-10-11 18:06:25"}
      * logs.json: {"logID": 9966,"userID": 99, "time": "2016-10-17 15:42:46", "typed": 0, "consumed": 0.0}
      */

//    val usersInfoJsonDF = spark.read.json(dataPath + "/users.json")
//    usersInfoJsonDF.createOrReplaceTempView("users")
//    val usersInfoSqlDF = spark.sql(
//      "SELECT userID as UserID, name as UserName, registeredTime as RegisteredTime from users")
//    usersInfoSqlDF.select("UserID", "UserName", "RegisteredTime").write.parquet(dataPath+"/userparquet.parquet")
//
//    val usersAccessLogJsonDF = spark.read.json(dataPath + "/logs.json")
//    usersAccessLogJsonDF.createOrReplaceTempView("logs")
//    val usersAccessLogSQLDF = spark.sql(
//      "SELECT logID, userID as userID, time, typed, consumed from logs")
//    usersAccessLogSQLDF.select("logID", "userID" , "time", "typed", "consumed").write.parquet(dataPath+"/logparquet.parquet")


    /**
      * Usually read the parquet source file in Production
      */
    val usersInfo = spark.read.parquet(dataPath + "/userparquet.parquet")
    val usersAccessLog = spark.read.parquet(dataPath + "/logparquet.parquet")


    /**
      * filter 之后进行 join, 然后进行 groupBy，groupBy 之后进行 agg，最后 sort
      */

    println("Questions 1:")
    usersAccessLog.filter("time >= 2016-10-01 and time <= 2016-10-10")
      .join(usersInfo, usersAccessLog("UserID") === usersInfo("UserID")) //
      .groupBy(usersInfo("UserID"), usersInfo("UserName"))
      .agg(count(usersAccessLog("time")).alias("userCount"))
      .sort($"userCount".desc)
      .limit(10)
      .show()

    println("Questions 2:")
    usersAccessLog.filter("time >= 2016-10-01 and time <= 2016-10-10")
      .join(usersInfo, usersAccessLog("UserID") === usersInfo("UserID"))
      .groupBy(usersInfo("UserID"), usersInfo("UserName"))
      .agg(count(usersAccessLog("consumed")).alias("totalCount"))
      .sort($"totalCount".desc)
      .limit(10)
      .show()

    println("Questions 3:")
    usersAccessLog.filter("time >= 2016-10-01 and time <= 2016-10-10")
      .join(usersInfo, usersAccessLog("UserID") === usersInfo("UserID"))
      .groupBy(usersInfo("UserID"), usersInfo("UserName"))
      .agg(count(usersAccessLog("consumed")).alias("totalCount"))
      .sort($"totalCount".desc)
      .limit(10)
      .show()

    println("Questions 4:")
    val userAccessTemp = usersAccessLog.as[UserLog].filter("time >= 2016-10-11 and time <= 2016-10-20")
      .map(log => LogOnce(log.logID, log.UserID, 1))
      .union(
        usersAccessLog.as[UserLog].filter("time >= 2016-10-11 and time <= 2016-10-20")
          .map(log => LogOnce(log.logID, log.UserID, -1))
      )
    userAccessTemp.join(usersInfo, usersAccessLog("UserID") === usersInfo("UserID"))
      .groupBy(usersInfo("UserID"), usersInfo("UserID"))
      .agg(sum(userAccessTemp("count")).alias("viewIncreaseTmp"))
      .sort($"viewIncreaseTmp".desc)
      .limit(10)
      .show()

    spark.stop()




  }
}
