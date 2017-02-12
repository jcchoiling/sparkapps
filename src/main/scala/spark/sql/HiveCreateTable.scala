package spark.sql

/**
  * Created by jcchoiling on 9/2/2017.
  */
object HiveCreateTable extends App{

//  val url = "jdbc:hive2://master:10000/default"
//  val driver = "org.apache.hive.jdbc.HiveDriver"
//  val username = "root"
//  val password = ""
//  var conn:Connection = _
//  var res: ResultSet = _

  try {

//    Class.forName(driver)
//    conn = DriverManager.getConnection(url, username, password)
//    val stmt = conn.createStatement

    /**
      * 調用 HiveScript
      */

    val sqlFile = "src/main/resources/spark.txt"
    val line = scala.io.Source.fromFile(sqlFile).getLines()
    line.foreach(println)

//    println("Hive Driver Connection Succeed")


//    stmt.close
//
//    conn.close

  } catch {
    case e: Exception => e.printStackTrace
  }
}
