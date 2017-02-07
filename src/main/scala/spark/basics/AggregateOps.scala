package basics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jcchoiling on 21/1/2017.
  */
object AggregateOps {

  def main(args: Array[String]) {

    val data = "src/main/resources/readme.md"
    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc = new SparkContext(conf)

    val line = sc.textFile(data)
    val flatLine = line.flatMap(x => x.split(" "))
    val mapWord = flatLine.map(x => (x,1))
    val aggregrateWord = mapWord.aggregateByKey(0)(
      (v1: Int, v2: Int) => v1 + v2,
      (v1: Int, v2: Int) => v1 + v2
    )

    aggregrateWord.collect().foreach(println)

    sc.stop()

  }
}
