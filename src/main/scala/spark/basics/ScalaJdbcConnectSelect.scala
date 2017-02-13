package database

import java.sql.{Connection,DriverManager}

/**
  * Created by jcchoiling on 9/2/2017.
  */
object ScalaJdbcConnectSelect extends App{

  val url = "jdbc:postgresql://localhost:5432/ott"
  val driver = "org.postgresql.Driver"
  val username = "postgres"
  val password = "aa123456"
  var connection:Connection = _

  try {

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    val statement = connection.createStatement
    val rs = statement.executeQuery("SELECT name, age FROM person")
    while (rs.next) {
      val name = rs.getString("name")
      val age = rs.getString("age")
      println("name = %s, age = %s".format(name,age))
    }

  } catch {
    case e: Exception => e.printStackTrace
  }

  connection.close
}