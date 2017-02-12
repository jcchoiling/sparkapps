package com.dt.spark.sparksql

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
object EB_Users_Analyzer_DateSet {


  def main(args: Array[String]){




    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[8]" //默认程序运行在本地Local模式中，主要学习和测试；
    var dataPath = "data/moviedata/medium/"  //数据存放的目录；

    /**
      * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
      * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
      */
    if(args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }


    /**
      * 创建Spark会话上下文SparkSession和集群上下文SparkContext，在SparkConf中可以进行各种依赖和参数的设置等，
      * 大家可以通过SparkSubmit脚本的help去看设置信息，其中SparkSession统一了Spark SQL运行的不同环境。
      */
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer_DataSet")

    /**
      * SparkSession统一了Spark SQL执行时候的不同的上下文环境，也就是说Spark SQL无论运行在那种环境下我们都可以只使用
      * SparkSession这样一个统一的编程入口来处理DataFrame和DataSet编程，不需要关注底层是否有Hive等。
      */
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext //从SparkSession获得的上下文，这是因为我们读原生文件的时候或者实现一些Spark SQL目前还不支持的功能的时候需要使用SparkContext

    import org.apache.spark.sql.functions._ //今天（2017年1月21）的第一个作业：通读functions.scala的源码
    import spark.implicits._

    //今天（2017年1月21）的第二个作业：自己根据我们的电商业务分析需要手动造出数据，需要注意的是任何实际生产环境的系统都不止一个数据文件或者不止一张表
    //例如我们这里的电商用户行为分析系统肯定至少有用户的信息usersInfo，同时肯定至少有用户访问行为信息usersAccessLog

    /**
      * 功能一：特定时间段内用户访问电商网站排名TopN:
      *   第一问题：特定时间段中的时间是从哪里来的？一般都是来自于J2EE调度系统，例如一个营销人员通过系统传入了2017.01.01~2017.01.10；
      *   第二问题：计算的时候我们会使用哪些核心算子：join、groupBy、agg（在agg中可以使用大量的functions.scala中的函数极大方便快速的实现
      *     业务逻辑系统）；
      *   第三个问题：计算完成后数据保存在哪里？现在生产环境一下是保存在DB、HBase/Canssandra、Redis等；
      *
      */

    val usersInfo = spark.read.format("json").json("Json file's path...")
    val usersAccessLog = spark.read.format("json").json("Json file's path...")

    usersAccessLog.filter("time >= 2017.01.01 and time <=  2017.01.10")
        .join(usersInfo, usersAccessLog("UserID") === usersInfo("UserID"))
        .groupBy(usersInfo("UserID"), usersInfo("UserName"))
        .agg(count(usersAccessLog("time")).alias("userCount"))
        .sort($"userCount".desc)
        .limit(10)
        .show()


    //    while(true){} //和通过Spark shell运行代码可以一直看到Web终端的原理是一样的，因为Spark Shell内部有一个LOOP循环

    sc.stop()


  }
}
