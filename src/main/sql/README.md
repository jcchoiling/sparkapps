# SQL
This section serves as a quick guide for looking up the sql synatx

## PostgresSQL


## MySQL



## Hive
Once you execute the create table synatx, you can check it in the HDFS, e.g. /user/hive/warehouse.
It will create a folder displaying the same name.
My local Hadoop is on http://master:50070/explorer.html#/user/hive/warehouse
Hive Web Interface: http://master:9999/hwi/

create external table need to define the location path too, it is to avoid deleting the data in Hive will accidentally delete
the data file in Hadoop. When you re-run the create external table script. The data will
If don't define location path during create external table. The data file in the hdfs will disappeared, becuase the file has already been loaded into Hive


### Steps to enable HWI
1) Download the source file and make the war file: jar cfM hive-hwi-2.1.0.war -C web .
2) scp to $HIVE_HOME/lib
3) Configure the hive-site.xml at $HIVE_HOME/conf
    - Configure the hwi war location <name>hive.hwi.war.file</name>
    - Configure the hwi port, default to 9999 <name>hive.hwi.listen.port</name>
4) copy the tool.jar from $JAVA_HOME/lib to $HIVE_HOME/lib
5) start hwi service: hive --service hwi



### Hive common syntax
SHOW TABLES; --show all the tables that exists in Hive




## HDFS
hdfs dfs -ls /
hdfs dfs -mkdir /user/data/staging
hdfs dfs -put $HIVE_HOME/examples/files/employee_part.txt /user/data/staging
