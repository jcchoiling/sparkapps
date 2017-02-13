package spark.basics

import org.apache.spark.sql.SparkSession

/**
  * Created by jcchoiling on 21/1/2017.
  *
  * Join 类似于数据库中的 inner join，会把相同的 key 关连然后输出一条一条的数据
  *
  * val a = Array((1,"spark"),(2,"hadoop"),(3,"scala"),(4,"Java"))
  * val b = Array((1,100),(2,90),(3,90),(1,99))
  *
  * Join Results:
  * (1,(spark,100))
  * (3,(scala,90))
  * (2,(hadoop,90))
  *
  * Join 的内部调用了 cogroup(), cartisian() and flatMap()
  * val a = Array((1,"Spark"),(2,"Hadoop"),(3,"Tachyon"))
  * val b = Array((1,100),(2,90),(3,70),(1,110),(2,95),(2,60))
  *
  * Join Results:
  * (1,(Spark,100))
  * (1,(Spark,110))
  * (3,(Tachyon,70))
  * (2,(Hadoop,90))
  * (2,(Hadoop,95))
  * (2,(Hadoop,60))
  *
  */
object JoinOps {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("JoinOps")
      .master("local")
      .getOrCreate()


//    val aRDD = spark.sparkContext.parallelize(Array((1,"spark"),(2,"hadoop"),(3,"scala"),(4,"Java")))
//    val bRDD = spark.sparkContext.parallelize(Array((1,100),(2,90),(3,90),(1,99)))

    val aRDD = spark.sparkContext.parallelize(Array((1,"Spark"),(2,"Hadoop"),(3,"Tachyon")))
    val bRDD = spark.sparkContext.parallelize(Array((1,100),(2,90),(3,70),(1,110),(2,95),(2,60)))

    aRDD.join(bRDD).foreach(item => {
      println(item)
    })

    spark.stop()


  }
}
