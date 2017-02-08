package basics;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 * Created by janicecheng on 8/2/2017.
 */


public class HiveCreateTable {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {

        // Register driver and create driver instance
//        Class.forName(driverName);

        // get connection
        Connection con = DriverManager.getConnection(
                "jdbc:hive://master:10000/default",
                "hive",
                "hive123");

        // create statement
        Statement stmt = con.createStatement();

        // execute statement
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS "
                +" employee ( eid int, name String, "
                +" salary String, destignation String)"
                +" COMMENT 'Employee details'"
                +" ROW FORMAT DELIMITED"
                +" FIELDS TERMINATED BY '\t'"
                +" LINES TERMINATED BY '\n'"
                +" STORED AS TEXTFILE;");

        System.out.println("Table employee created");
        con.close();
    }
}
