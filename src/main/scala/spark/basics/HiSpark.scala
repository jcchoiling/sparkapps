package spark.basics

import org.apache.spark.sql.SparkSession

/**
  * Created by jcchoiling on 6/2/2017.
  */


object HiSpark {

  case class Person (name: String, age: Long)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HiSpark")
      .master("local")
      .config("spark.sql.warehouse","spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val employee = spark.read.json("src/main/resources/people.json")

    val employeeDS = employee.as[Person]
    employeeDS.show()
    employeeDS.printSchema()

    val employeeDF = employeeDS.toDF()


  }

}
