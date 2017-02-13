package spark.basics

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jcchoiling on 22/1/2017.
  */

object DataSetOps {

  case class Person(name: String, age: Long)
  case class Score (n: String, score: Long) // 这里的名称要和 peopleScore.json 的列名一样，或者会报错!

  def main(args: Array[String]) {

    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("DataSetOps")
      .master("local")
      .config("spark.sql.warehouse.dir",warehouseLocation) //
      .getOrCreate()

    /**
      * DataSet中的transformation和Action的操作，Action中的操作有以下几种:
      * show(), collect(), first(), reduce(), take(), count(), foreach()
      * 这些操作都会产生结果，也就是说会执行逻辑的计算过程
      */

    import spark.implicits._
    import org.apache.spark.sql.functions._


    /**
      * DataFrame
      */
    val personDF = spark.read.json("src/main/resources/people.json") //DataFrame
    val personScoreDF = spark.read.json("src/main/resources/peopleScore.json")

    //    personDF.join(personScoreDF, $"name" === $"n").show()

    personDF.filter($"age" > 20).join(personScoreDF, $"name" === $"n")
      .groupBy(personDF("name")).agg(avg(personScoreDF("score")),avg(personDF("age"))).show()

    /*
    SELECT
      p1.name,
      avg(p2.score),
      avg(p1.age)
    FROM personDF p1
    JOIN personScoreDF p2
    WHERE p1.name = p2.name
    GROUP BY p1.name
     */


    //    personDF.select("name").show()
    //
    //    personDF.show()
    //
    //    personDF.take(2).foreach(println)
    //    personDF.collect().foreach(println)
    //
    //    println(personDF.count())
    //    println(personDF.first())
    //
    //    personDF.printSchema()
    //    personDF.foreach(p => println(p)) // personDF.foreach(println(_))
    //    print(personDF.map(person => 1).reduce(_+_))


    /**
      * DataFrame
      */
    val personDS = personDF.as[Person] // 因为DataSet 是强类型的，所以必需先定义 case class
    val personScoreDS = personScoreDF.as[Score]


    //    personDS.groupBy($"name")
    //      .agg(collect_list($"name"), collect_set($"name")) //collect_list() 把数据放在一个List中，collect_set() 把数据放在一个Set中
    //      .collect()
    //      .foreach(println)

    /*
    Results:
    [Michael,WrappedArray(Michael, Michael, Michael),WrappedArray(Michael)]
    [Andy,WrappedArray(Andy, Andy, Andy),WrappedArray(Andy)]
    [Justin,WrappedArray(Justin, Justin, Justin),WrappedArray(Justin)]
     */

    personDS.groupBy($"name")
      .agg(
        sum($"age"),
        avg($"age"),
        max($"age"),
        min($"age"),
        count($"age"),
        countDistinct($"age"),
        mean($"age"),
        current_date()
      ).show()




    //    personDS.show()
    //    personDS.printSchema()
    //    personDS.toDF()


    //    personDS.map{ person =>
    //      (person.name, person.age + 100L)
    //    }.show()

    //    personDS.mapPartitions{people =>
    //      val res = ArrayBuffer[(String, Long)]()
    //
    //      while(people.hasNext){
    //        val person = people.next()
    //        res += ((person.name, person.age + 1000L))
    //      }
    //
    //      res.iterator
    //    }.show()

    /**
      * 如果把大分区变少分区，一般会使用 coalesce()
      */
        personDS.dropDuplicates("name").show()
    //    personDS.distinct().show()

    //    println(personDS.rdd.partitions.size) // original partition size is 1

    /**
      * 如果把大分区变少分区，一般会使用 coalesce()
      * 如果把小分区变大分区，一般会使用 repartition()
      */

    //    val repartionedDS = personDS.repartition(4) // 可以把分区变大，也可以把分区变少
    //    println(repartionedDS.rdd.partitions.size) // now repartition to 4
    //
    //    val coalescedDS = repartionedDS.coalesce(2) // 最合适的埸景是把多个数据分匿合并成少的分匿，中间不会进行 shuffle
    //    println(coalescedDS.rdd.partitions.size) // now repartition to 2


    /**
      * sort(), join(), joinWith(),randomSplit(), select(), groupBy(), agg(), col()
      */
    //    personDS.sort("age").show()
    //    personDS.sort($"age".desc).show()
    //    personDS.joinWith(personScoreDS, $"name" === $"n").show() // 第一个是personDS的列名，第二个是personScoreDS的列名
    //    personDS.randomSplit(Array(10,20)).foreach(dataset => dataset.show())
    //    personDS.sample(false, 0.5).show() // 随机抽样



    /**
      * DataFrame 使用 sql 的方式
      */
    //    personScoreDF.createOrReplaceTempView("score")
    //    personDF.createOrReplaceTempView("people")
    //    val sqlDF = spark.sql(
    //      "SELECT * FROM people WHERE age > 20"
    //    )
    //    sqlDF.show()
    //    sqlDF.explain()



    spark.stop()

  }
}
