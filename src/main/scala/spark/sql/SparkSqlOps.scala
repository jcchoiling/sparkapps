package spark.sql


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by jcchoiling on 16/3/2017.
  *
  * Steps 1: SparkSession as the entry point for SparkSQL, you need to create builder, set appname and configuration
  * Steps 2: You can read the source data with the following:
  *           1) spark.json.read() to read the source data in Json format
  * Steps 3: After creating DataFrame, df, you can call
  *           1) df.show()
  *           2) df.printSchema()
  *           3) df.select("name").show()
  *           4) df.filter($"age" > 25).select("name").show()
  *
  *
  */



object SparkSqlOps {

//  case class Person(name:String, age: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    //create a spark session as the entry point
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Basic Examples")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.types._

    val sc = spark.sparkContext

//    val df = spark.read.json("src/main/resources/general/people.json")
//    df.show() //displaying all the data
//    df.printSchema()
//    df.select("name").show()
//    df.select($"name",$"age" + 1).show()
//    df.filter($"age" > 25).select("name").show()
//    df.groupBy($"age").count().show()



    val schemaString = "name,age"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val personRDD = sc.textFile("src/main/resources/general/people.txt") //personRDD
    val rowRDD = personRDD.map(_.split(",")).map(attr => Row(attr(0),attr(1).trim()))
    val personDF = spark.createDataFrame(rowRDD,schema)

//    val personDF = sc.textFile("src/main/resources/general/people.txt") //personRDD
//      .map(x => x.split(",")) //Array[String] = Array(name, age)
//      .map(attr => Person(attr(0),attr(1).trim().toInt))
//      .toDF()


    personDF.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.map(people => "Name: " + people(0)).show()
//    sqlDF.show()


    spark.stop()


  }

}
