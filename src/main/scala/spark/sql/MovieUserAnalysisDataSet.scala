package spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


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

object MovieUserAnalysisDataSet {

  case class User(UserID: String, Gender: String, Age: String, OccupationID: String, Zip_Code: String)
  case class Ratings(UserID: String, MovieID: String, Rating: Double, Timestamp: String)
  case class Movies(MovieID: String, Title: String, Genres: String)
  case class Occupations(OccupationID: String, OccupationName: String)


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    var masterUrl = "local[1]"  //默认程序运行在本地Local模式中，主要是学习和测试
    var dataPath = "src/main/resources/moviedata/" //数据存放的目录

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalysisDataSet")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    /**
      * import some implicits package
      */

    import spark.implicits._ // Need to import the implicits, this is to transform the data to DatSets
    import spark.sql


    /**
      * Extract the data from the source file
      */
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    /**
      * Define the user schema structure: UserID::Gender::Age::OccupationID::Zip-code
      */
    val userSchema = StructType("UserID::Gender::Age::OccupationID::Zip_code".split("::")
      .map(col => StructField(col, StringType, true)))
    val usersRDDRows = usersRDD.map(_.split("::"))
      .map(attr => Row(attr(0).trim,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim))
    val usersDF = spark.createDataFrame(usersRDDRows, userSchema)
    val usersDataSet = usersDF.as[User]


    /**
      * Define the movie schema structure: MovieID::Title::Genres
      */
    val moviesSchema = StructType("MovieID::Title::Genres".split("::")
      .map(col => StructField(col, StringType, true)))
    val moviesRDDRows = moviesRDD.map(_.split("::"))
      .map(attr => Row(attr(0).trim,attr(1).trim,attr(2).trim))
    val moviesDF = spark.createDataFrame(moviesRDDRows, moviesSchema)
    val moviesDataSet = moviesDF.as[Movies]


    /**
      * Define the rating schema structure: UserID::MovieID::Rating::Timestamp
      */
    val ratingSchema = StructType("UserID::MovieID".split("::")
      .map(col => StructField(col, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)

    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(attr => Row(attr(0).trim,attr(1).trim,attr(2).trim.toDouble,attr(3).trim))
    val ratingsDF = spark.createDataFrame(ratingsRDDRows, ratingSchema)
    val ratingsDataSet = ratingsDF.as[Ratings]

    println("功能一：通过DataSet实现某特定电影观看者中男性和女性不同年龄分别有多少人: ")
    ratingsDF.filter(s" MovieID = 1193")
      .join(usersDF, "UserID")
      .select("Gender","Age")
      .groupBy("Gender","Age")
      .count()
      .show(10)

//    println("功能二：用GlobalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人？")
//    ratingsDF.createGlobalTempView("ratings_global")
//    usersDF.createGlobalTempView("users_global")
//
//    spark.sql(
//      "SELECT u.Gender, u.Age, count(*) " +
//      "FROM global_temp.ratings_global r " +
//      "JOIN global_temp.users_global u " +
//      "ON r.UserID = u.UserID " +
//      "WHERE r.MovieID = 1193 " +
//      "GROUP BY u.Gender, u.Age"
//    ).show(10)

//    ratingsDF.createOrReplaceTempView("ratings")
//    usersDF.createOrReplaceTempView("users")

//    spark.sql(
//      "SELECT u.Gender, u.Age, count(*) " +
//        "FROM ratings r " +
//        "JOIN users u " +
//        "ON r.UserID = u.UserID " +
//        "WHERE r.MovieID = 1193 " +
//        "GROUP BY u.Gender, u.Age"
//    ).show(10)

    ratingsDataSet.select("MovieID","Rating").groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc).show(10)

    // SQL logic: SELECT movie_id, count(1) FROM ratings GROUP BY movie_id ORDER BY count(1) DESC;
    println("通过DataSet對所有电影中粉丝或者观看人数最多的电影:")
    ratingsDataSet.select("MovieID")
      .groupBy("MovieID")
      .count()
      .orderBy($"count".desc)
      .show(10)


    val genderRatingDataSet = ratingsDF.join(usersDF, "UserID").cache()
    val maleFilteredRatingsDataSet = genderRatingDataSet.filter(s"Gender = 'M'")
    val femaleFilteredRatingsDataSet = genderRatingDataSet.filter(s"Gender = 'F'")

    println("纯粹使用DataSet实现所有电影中最受男性喜爱的电影Top10:")
    maleFilteredRatingsDataSet.groupBy("MovieID").avg("Rating")
      .orderBy($"avg(Rating)".desc).show(10)

    println("纯粹使用DataSet实现所有电影中最受女性喜爱的电影Top10:")
    femaleFilteredRatingsDataSet.groupBy("MovieID").avg("Rating")
      .orderBy($"avg(Rating)".desc, $"MovieID" ).show(10)


    /**
    (American Beauty (1999),715)
    (Star Wars: Episode VI - Return of the Jedi (1983),586)
    (Star Wars: Episode V - The Empire Strikes Back (1980),579)
    (Matrix, The (1999),567)
    (Star Wars: Episode IV - A New Hope (1977),562)
      */


    /**
      * orderBy 操作需要在 join() 之后
      */
    ratingsDataSet.join(usersDataSet,"UserID").filter("Age = '18'").groupBy("MovieID")
      .count().join(moviesDataSet,"MovieID").select("Title","count").orderBy($"count".desc).limit(10).show(5)

    ratingsDataSet.join(usersDataSet,"UserID").filter("Age = '25'").groupBy("MovieID")
      .count().join(moviesDataSet,"MovieID").select("Title","count").orderBy($"count".desc).show(5)

    spark.stop()




  }
}
