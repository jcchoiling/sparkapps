package spark.basics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jcchoiling on 18/2/2017.
  */
object TopNBasic {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val dataPath = "src/main/resources/" //数据存放的目录

    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(dataPath + "general/basicTopN.txt")

    val pairs = lines.map(line => (line.toInt, line)) //生成(Key,Value)

    val sortedPairs = pairs.sortByKey(false)

    val sortedData = sortedPairs.map(pair => pair._2)

    val top5 = sortedData.take(5)

    top5.foreach(println)

  }

}
