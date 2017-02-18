package spark.basics

/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by janicecheng on 17/1/2017.
  *
  * RangePartitioner 怎么工作
  *
  */
object SecondarySortApp {

  def main (args: Array[String]) :Unit = {

    val dataPath = "src/main/resources/" //数据存放的目录

    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(dataPath + "general/spark.txt")

    val pairWithSortKey = lines.map(line => {
      val splitted = line.split(" ")
      (new SecondarySort(splitted(0).toInt,splitted(1).toInt), line)
    })

    val sorted = pairWithSortKey.sortByKey(false)

    val sortedResults = sorted.map(sortedLine => sortedLine._2)

    sortedResults.collect().foreach(println)

    sc.stop()
  }
}

class SecondarySort(val first: Int, val second: Int) extends Ordered[SecondarySort] with Serializable {

  override def compare(that: SecondarySort): Int = {
    if (this.first - that.first !=0){
      this.first - that.first
    } else {
      this.second - that.second
    }
  }

}