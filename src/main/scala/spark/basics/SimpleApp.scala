package basics


/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by janicecheng on 17/1/2017.
  */
object SimpleApp {
  def main (args: Array[String]) :Unit = {

    val dataPath = "src/main/resources/" //数据存放的目录

    //            val conf = new SparkConf().setAppName("HelloSpark").setMaster("spark://HadoopM:7077")
    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc = new SparkContext(conf)

    val words = sc.textFile(dataPath + "spark.txt")
      .flatMap(_.split(" "))
      .map(m => (m,1)).reduceByKey(_+_)

    words.foreach(println)

    sc.stop()
  }
}