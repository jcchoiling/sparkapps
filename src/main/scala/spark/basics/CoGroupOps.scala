package spark.basics
import org.apache.spark.sql.SparkSession

/**
  * Created by jcchoiling on 21/1/2017.
  *
  * val a = Array((1,"Spark"),(2,"Hadoop"),(3,"Tachyon"))
  * val b = Array((1,100),(2,90),(3,70),(1,110),(2,95),(2,60))
  *
  * Cogroup_results:
  * (1,(CompactBuffer(Spark),CompactBuffer(100, 110)))
  * (3,(CompactBuffer(Tachyon),CompactBuffer(70)))
  * (2,(CompactBuffer(Hadoop),CompactBuffer(90, 95, 60)))
  *
  * cogroup 意思就是把相同 key 的 RDD 进行分组
  *
  * (1,("Spark",(100,110)))
  * (2,("Tachyon",(70,)))
  * (3,("Hadoop",(90,95,60)))
  *
  */
object CoGroupOps {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("CoGroup-transformation")
      .master("local")
      .getOrCreate()

    val aRDD = spark.sparkContext.parallelize(Array((1,"Spark"),(2,"Hadoop"),(3,"Tachyon")))
    val bRDD = spark.sparkContext.parallelize(Array((1,100),(2,90),(3,70),(1,110),(2,95),(2,60)))

    aRDD.cogroup(bRDD).foreach(item => {
      println(item)
    })

    spark.stop()

  }
}
