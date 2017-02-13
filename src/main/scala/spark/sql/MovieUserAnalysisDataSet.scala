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
      * Extract the data from the source file
      */
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    /**
      * Define the user schema structure: UserID::Gender::Age::OccupationID::Zip-code
      */
    val userSchema = StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::")
      .map(col => StructField(col, StringType, true)))
    val usersRDDRows = usersRDD.map(_.split("::"))
      .map(attr => Row(attr(0).trim,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim))
    val usersDF = spark.createDataFrame(usersRDDRows, userSchema)



    /**
      * Define the movie schema structure: MovieID::Title::Genres
      */
    val moviesSchema = StructType("MovieID::Title::Genres".split("::")
      .map(col => StructField(col, StringType, true)))
    val moviesRDDRows = moviesRDD.map(_.split("::"))
      .map(attr => Row(attr(0).trim,attr(1).trim,attr(2).trim))
    val moviesDF = spark.createDataFrame(moviesRDDRows, moviesSchema)


    /**
      * Define the rating schema structure: UserID::MovieID::Rating::Timestamp
      */
    val ratingSchema = StructType("UserID::MovieID".split("::")
      .map(col => StructField(col, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)

//    val ratingSchema = StructType(
//      "UserID::MovieID::Rating::Timestamp".split("::").map{
//        case "UserID" => StructField("UserID", StringType, true)
//        case "MovieID" => StructField("MovieID", StringType, true)
//        case "Rating" => StructField("Rating", DoubleType, true)
//        case "Timestamp" => StructField("Timestamp", StringType, true)
//    })

    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(attr => Row(attr(0).trim,attr(1).trim,attr(2).trim.toDouble,attr(3).trim))
    val ratingsDF = spark.createDataFrame(ratingsRDDRows, ratingSchema)

//    println("功能一：通过DataFrame实现某特定电影观看者中男性和女性不同年龄分别有多少人: ")
//    ratingsDF.filter(s" MovieID = 1193")
//      .join(usersDF, "UserID")
//      .select("Gender","Age")
//      .groupBy("Gender","Age")
//      .count()
//      .show(10)
//
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


//    ratingsDF.select("MovieID","Rating").groupBy("MovieID").avg("Rating").rdd // 这里有二列元素
//    .map(row => (row(1),(row(0),row(1))))
//      .sortBy(_._1.toString.toDouble, false)
//      .map(tuple => tuple._2)
//      .take(10)
//      .foreach(println)


    import spark.implicits._ // Need to import the implicits, this is to transform the data to DatSets
//    ratingsDF.select("MovieID","Rating").groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc).show(10)

    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),x(2))).cache() //UserID::MovieID::Rating

//    println("通过RDD對所有电影中粉丝或者观看人数最多的电影:")
//    ratings.map(x => (x._2, 1)).reduceByKey(_+_)
//      .map(x => (x._2, x._1))
//      .sortByKey(false)
//      .map(x => (x._2, x._1))
//      .take(10)
//      .foreach(println)
//
//    println("通过DataFrame和RDD结合對所有电影中粉丝或者观看人数最多的电影:")
//    ratingsDF.select("MovieID").groupBy("MovieID")
//      .count().rdd
//      .map(row => (row(1),(row(0),row(1))))
//      .sortBy(_._1.toString.toLong, false)
//      .map(tuple => tuple._2)
//      .collect()
//      .take(10)
//      .foreach(println)
//
//    // SQL logic: SELECT movie_id, count(1) FROM ratings GROUP BY movie_id ORDER BY count(1) DESC;
//    println("通过DataFrame對所有电影中粉丝或者观看人数最多的电影:")
//    ratingsDF.select("MovieID")
//      .groupBy("MovieID")
//      .count()
//      .orderBy($"count".desc)
//      .show(10)

//
//    val genderRatingDF = ratingsDF.join(usersDF, "UserID").cache()
//    val maleFilteredRatingsDF = genderRatingDF.filter(s"Gender = 'M'")
//    val femaleFilteredRatingsDF = genderRatingDF.filter(s"Gender = 'F'")
//
//    maleFilteredRatingsDF.groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc).show(10)
//
//    println("纯粹使用DataFrame实现所有电影中最受女性喜爱的电影Top10:")
//
//    femaleFilteredRatingsDF
//      .groupBy("MovieID")
//      .avg("Rating")
//      .orderBy($"avg(Rating)".desc, $"MovieID" )
//      .show(10)


    // MapJoin
    val targetQQUsers = usersRDD.map(_.split("::")).map(x =>(x(0),x(2))).filter(_._2.equals("18")) //(UserId,Age)
    val targetTaobaoUsers = usersRDD.map(_.split("::")).map(x => (x(0),x(2))).filter(_._2.equals("25")) //(UserId,Age)


    val targetQQUsersSet = mutable.HashSet() ++ targetQQUsers.map(_._1).collect() //[Ljava.lang.String;@5bc28f40
    val targetTaobaoUsersSet = mutable.HashSet() ++ targetTaobaoUsers.map(_._1).collect()


    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUsersSet)

    //"movies.dat"：MovieID::Title::Genres
    val movieID2Name = moviesRDD.map(_.split("::")).map(x => (x(0),x(1))).collect().toMap //把它变成 Map 的方式

    ratingsRDD.map(_.split("::")).map(x => (x(0),x(1))).filter(
      x => targetQQUsersBroadcast.value.contains(x._1) // userId,MovieId: (18,2987)
    )
      .map(x => (x._2,1))
      .reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2,x._1))
      .take(5)
      .map(x => (movieID2Name.getOrElse(x._1, null),x._2))
      .foreach(println)
    /**
    (American Beauty (1999),715)
    (Star Wars: Episode VI - Return of the Jedi (1983),586)
    (Star Wars: Episode V - The Empire Strikes Back (1980),579)
    (Matrix, The (1999),567)
    (Star Wars: Episode IV - A New Hope (1977),562)
      */

    println("纯粹通过RDD的方式实现所有电影中淘宝核心目标用户最喜爱电影TopN分析:")
    ratingsRDD.map(_.split("::")).map(x => (x(0),x(1))).filter(
      x => targetTaobaoUsersBroadcast.value.contains(x._1) // userId,MovieId: (18,2987)
    )
      .map(x => (x._2,1))
      .reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2,x._1))
      .take(5)
      .map(x => (movieID2Name.getOrElse(x._1, null),x._2))
      .foreach(println)

    /**
      * orderBy 操作需要在 join() 之后
      */
    ratingsDF.join(usersDF,"UserID").filter("Age = '18'").groupBy("MovieID")
      .count().join(moviesDF,"MovieID").select("Title","count").orderBy($"count".desc).show(5)

    ratingsDF.join(usersDF,"UserID").filter("Age = '25'").groupBy("MovieID")
      .count().join(moviesDF,"MovieID").select("Title","count").orderBy($"count".desc).show(5)

    spark.stop()




  }
}
