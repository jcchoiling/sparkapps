package spark.sql

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * Created by jcchoiling on 9/2/2017.
  */
object TestHiveConnection extends App{
  
  val url = "jdbc:hive2://master:10000/default"
  val driver = "org.apache.hive.jdbc.HiveDriver"
  val username = "root"
  val password = ""
  var conn:Connection = _
  var res: ResultSet = _

  try {
    /**
      * 第一步：把JDBC驱动通过反射的方式加载进来
      */
    Class.forName(driver)

    /**
      * 第二步：通过JDBC建立和Hive的连接器，默认端口是10000，默认用户名和密码都为空
      */
    conn = DriverManager.getConnection(url, username, password)

    /**
      * 第三步：创建Statement句柄，基于该句柄进行SQL的各种操作；
      */
    val stmt = conn.createStatement

    /**
      * 第4.1步骤：建表Table,如果已经存在的话就要首先删除；
      */
    val tableName = "HiveDriverTable"
    stmt.execute("DROP TABLE IF EXISTS " + tableName)
    stmt.execute("CREATE TABLE " + tableName + " (id INT, name STRING)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\,' LINES TERMINATED BY '\n'")

    /**
      * 第4.2步骤：查询建立的Table；
      */
    var sql = "SHOW TABLES " + tableName
    println("Running: " + sql)
    res = stmt.executeQuery(sql)
    while (res.next) println(res.getString(1))

    /**
      * 第4.3步骤：查询建立的Table的schema；
      */
    sql = "DESCRIBE " + tableName
    println("Running: " + sql)
    res = stmt.executeQuery(sql)
    while (res.next) println(res.getString(1) + "\t" + res.getString(2))

    /**
      * 第4.4步骤：加载数据进入Hive中的Table;
      */
    val filepath = "/usr/local/hive/hive210/examples/files/testHiveDriver.txt"
    sql = "LOAD DATA LOCAL INPATH '" + filepath + "' INTO TABLE " + tableName
    println("Running: " + sql)
    stmt.execute(sql)

    /**
      * 第4.5步骤：查询进入Hive中的Table的数据；
      */
    sql = "SELECT * FROM " + tableName
    println("Running: " + sql)
    res = stmt.executeQuery(sql)
    while (res.next) println(res.getString(1) + "\t" + res.getString(2))

    /**
      * 第4.6步骤：Hive中的对Table进行统计操作；
      */
    sql = "SELECT COUNT(1) FROM " + tableName
    println("Running: " + sql)
    res = stmt.executeQuery(sql)
    while (res.next) println("Total lines :" + res.getString(1))

    /**
      * 第4.7步骤：成功测试完毕删除掉表单
      */
    sql = "DROP TABLE IF EXISTS " + tableName
    println("Dropping table: " + tableName)
    stmt.execute(sql)

    println("Hive Driver Connection Succeed")


    stmt.close

    conn.close

  } catch {
    case e: Exception => e.printStackTrace
  }
}
