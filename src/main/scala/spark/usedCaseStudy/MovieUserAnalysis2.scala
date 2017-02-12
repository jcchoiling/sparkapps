package spark.usedCaseStudy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

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
object MovieUserAnalysis2 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    var masterUrl = "local[1]"  //默认程序运行在本地Local模式中，主要是学习和测试
    var dataPath = "src/main/resources/moviedata/" //数据存放的目录

    /**
      * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
      * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
      */
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }


    /**
      * 创建Spark集群上下文sc，在sc中可以进行各种依赖和参数的设置等，大家可以通过SparkSubmit脚本的help去看设置信息；
      */
    val conf = new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalysis")
    val sc = new SparkContext(conf)


    /**
      * 第一步：读取数据，用什么方式读取数据呢？在这里是使用RDD!
      */
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    val usersBasic = usersRDD.map(_.split("::")).map {user =>(  //UserID::Gender::Age::OccupationID
      user(3),(user(0), user(1), user(2))
      )
    }

    val occupation = occupationsRDD.map(_.split("::")).map{job =>
      (job(0),job(1))
    }

    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1))).filter(_._2.equals("1193")) //UserID::MovieID

//    ratings.take(10).foreach(println)

    val userInformation = usersBasic.join(occupation) //(10,((1982,M,1,10),(10,K-12 student)))
    userInformation.cache()

    // (12,(1193,((8,M,25,12),(12,programmer))))
    ratings.join(userInformation).map(x => (x._1,x._2._1,x._2._2._2)).take(10).foreach(println) //(12,1193,programmer)





    sc.stop()


  }
}
