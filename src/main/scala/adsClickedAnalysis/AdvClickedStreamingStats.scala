package adsClickedAnalysis

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jcchoiling on 7/2/2017.
  */
object AdvClickedStreamingStats {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("AdvClickedStreamingStats")
      .setJars(List(
        "/usr/local/spark/spark201/jars/spark-streaming_2.11-2.0.1.jar"
      ))

    val sc = new SparkContext(conf)





  }
}
