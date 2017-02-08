# SQL
This section serves as a quick guide for looking up the sql synatx

## PostgresSQL


## MySQL



## Hive
Once you execute the create table synatx, you can check it in the HDFS, e.g. /user/hive/warehouse.
It will create a folder displaying the same name. My local Hadoop is on http://master:50070/explorer.html#/user/hive/warehouse

create external table need to define the location path too, it is to avoid deleting the data in Hive will accidentally delete
the data file in Hadoop. When you re-run the create external table script. The data will
If don't define location path during create external table. The data file in the hdfs will disappeared, becuase the file has already been loaded into Hive


SHOW TABLES; --describe table




## HDFS
hdfs dfs -ls /
hdfs dfs -mkdir /user/data/staging
hdfs dfs -put $HIVE_HOME/examples/files/employee_part.txt /user/data/staging
