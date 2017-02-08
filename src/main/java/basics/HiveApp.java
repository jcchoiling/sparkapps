package basics;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 * Created by janicecheng on 8/2/2017.
 */


public class HiveApp {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://master:10000/default";
    private static String user = "root"; //一般情况下可以使用匿名的方式，在这里用了root是因为整个Hive的所有安装等操作都是root
    private static String password = "";

    public static void main(String[] args) throws SQLException {

        ResultSet res = null;


        try {
            /**
             * 第一步：把JDBC驱动通过反射的方式加载进来
             */
            Class.forName(driverName);

            /**
             * 第二步：通过JDBC建立和Hive的连接器，默认端口是10000，默认用户名和密码都为空
             */
            Connection con = DriverManager.getConnection(url, user, password);

            /**
             * 第三步：创建Statement句柄，基于该句柄进行SQL的各种操作；
             */
            Statement stmt = con.createStatement();

            /**
             * 接下来就是SQL的各种操作；
             * 第4.1步骤：建表Table,如果已经存在的话就要首先删除；
             */
            String tableName = "HiveDriverTable";
            stmt.execute("DROP TABLE IF EXISTS " + tableName );
            stmt.execute("CREATE TABLE " + tableName + " (id INT, name STRING)"
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\,' LINES TERMINATED BY '\n'"
            );

            /**
             *  第4.2步骤：查询建立的Table；
             */
            String sql = "SHOW TABLES " + tableName ;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }

            /**
             *  第4.3步骤：查询建立的Table的schema；
             */
            sql = "DESCRIBE " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }

            /**
             *  第4.4步骤：加载数据进入Hive中的Table；
             */
            String filepath = "/usr/local/hive210/examples/files/testHiveDriver.txt";
            sql = "LOAD DATA LOCAL INPATH '" + filepath + "' INTO TABLE " + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);


            /**
             *  第4.5步骤：查询进入Hive中的Table的数据；
             */
            sql = "SELECT * FROM " + tableName ;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
            }

            /**
             *  第4.6步骤：Hive中的对Table进行统计操作；
             *  這里会产生 mapReduce Job
             */
            sql = "SELECT count(1) FROM " + tableName ;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println("Total lines :" + res.getString(1));
            }

            /**
             *  第4.7步骤：成功测试完毕删除掉表单
             */
            sql = "DROP TABLE IF EXISTS " + tableName ;
            System.out.println("Dropping table: " + tableName);
            stmt.execute(sql);

            System.out.println("Hive Driver Connection Succeed");

            stmt.close();
            
            con.close();

        } catch (Exception e){

            e.printStackTrace();

        }




    }
}
