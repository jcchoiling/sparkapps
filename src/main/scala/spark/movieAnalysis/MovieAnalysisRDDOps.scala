package spark.movieAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by jcchoiling on 8/2/2017.
  *
  *
  *
  */
object MovieAnalysisRDDOps {
  def main(args: Array[String]): Unit = {

    val masterUrl = "local[1]"
    val dataPath = "src/main/resources/moviedata/"

    val conf = new SparkConf().setMaster(masterUrl).setAppName("MovieAnalysisRDDOps")
    val sc = new SparkContext(conf)

    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    //UserID::Gender::Age::Occupation::Zip-code (1,F,1)
    val usersFormatted = usersRDD.map(_.split("::")).map(line => (line(0),(line(1),line(2))))

    //UserID::MovieID::Rating::Timestamp (1,1)
    val ratingsFiltered = ratingsRDD.map(_.split("::")).map(line => (line(0),line(1))).filter(_._2.equals("1"))

    val joined = ratingsFiltered.join(usersFormatted) //(4425,(1,(M,35)))

    val reducedResutls = joined.map(line => (line._2._2, 1)).reduceByKey(_+_) //((F,50),30)

    reducedResutls.collect().take(10).foreach(println)


  }

}
