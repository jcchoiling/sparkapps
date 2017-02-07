package basics

import org.apache.spark.sql.SparkSession

/**
  * Created by jcchoiling on 22/1/2017.
  */
object HiveOps {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("HiveOps")
      .master("local")
      .config("spark.sql.warehouse","spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql


    /**
      *  HiveQL Context
      */

    sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/employee.txt' INTO TABLE employee")

    sql("SELECT * FROM employee").show()

//      +----+--------+----+
//      |  id|    name| age|
//      +----+--------+----+
//      |1201|  satish|null|
//      |1202| krishna|null|
//      |1203|   amith|null|
//      |1204|   javed|null|
//      |1205|  prudvi|null|
//      +----+--------+----+

    sql("CREATE TABLE IF NOT EXISTS person (name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/people.txt' INTO TABLE person")

    sql("SELECT * FROM person").show()

//      +-----------+----+
//      |       name| age|
//      +-----------+----+
//      |Michael, 29|null|
//      |   Andy, 30|null|
//      |   Andy, 30|null|
//      | Justin, 19|null|
//      +-----------+----+

    sql("CREATE TABLE IF NOT EXISTS kv (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE kv")

    sql("SELECT * FROM kv").show()

//      +---+-------+
//      |key|  value|
//      +---+-------+
//      |238|val_238|
//      | 86| val_86|
//      |311|val_311|
//      | 27| val_27|
//      |165|val_165|
//      |409|val_409|
//      |255|val_255|
//      |278|val_278|
//      | 98| val_98|
//      |484|val_484|
//      |265|val_265|
//      |193|val_193|
//      |401|val_401|
//      |150|val_150|
//      |273|val_273|
//      |224|val_224|
//      |369|val_369|
//      | 66| val_66|
//      |128|val_128|
//      |213|val_213|
//      +---+-------+


    spark.stop()


  }

}
