# SQL
This section serves as a quick guide for looking up the sql syntax.

## PostgresSQL

    psql -h localhost -d postgres -U postgres

## MySQL
[to be updated]



## Hive
1. Once you execute the create table synatx, you can check it in the HDFS, e.g. /user/hive/warehouse. It will create a folder displaying the same name. 

    My local Hadoop is on http://master:50070/explorer.html#/user/hive/warehouse Hive Web Interface: http://master:9999/hwi/
2. Create external table need to define the location path too, it is to avoid deleting the data in Hive will accidentally delete the data file in Hadoop. When you re-run the create external table script. The data will removed if don't define location path during create external table. The data file in the hdfs will disappeared, becuase the file has already been loaded into Hive


#### Steps to enable HWI
* Download the source file and make the war file: jar cfM hive-hwi-2.1.0.war -C web .
* scp to $HIVE_HOME/lib
* Configure the hive-site.xml at $HIVE_HOME/conf
    * Configure the hwi war location <name>hive.hwi.war.file</name>
    * Configure the hwi port, default to 9999 <name>hive.hwi.listen.port</name>
*   copy the tool.jar from JAVA_HOME/lib to $HIVE_HOME/lib
*   start hwi service: hive --service hwi

#### Steps to enable hive metastore and hiveServer2
    hive --service hiveserver2
    nohup hive --service metastore &

Check the jdbc login

    beeline -u "jdbc:hive2://<localhost>:<port>/<dbname>" -n <username> -p <password> -d <class.jar>
    $HIVE_HOME/bin/beeline -u "jdbc:hive2://localhost:10000/default" -n root
*Reference: http://www.cnblogs.com/zhangeamon/p/5787365.html. only for testing purpose, not suggest to use root in production*



#### Hive common syntax
    SHOW tables; --show all the tables that exists in Hive
    DESC table; --describe the table structure
    SHOW PARTITIONS table_partitioned; --display the table partition information
    ALTER TABLE table_partitioned ADD PARTITION (col1='20170101', col2='China')

#### Hive Script

    hive -f <hive-script.sql> -- execute the hive script
    hive -S -e "SELECT * FROM worker" >> /usr/local/hive210/hiveTmp/hiveresults.txt
    set.hive.map.aggr = true
    set hive.cli.print.current.db=true; --display the hive current db
    set hive.exec.mode.local.auto=true; --
    



## HDFS
Start and stop the Hadoop Cluster 

    $HADOOP_HOME/sbin/start-dfs.sh
    $HADOOP_HOME/sbin/start-yarn.sh
    $HADOOP_HOME/sbin/stop-dfs.sh
    $HADOOP_HOME/sbin/stop-yarn.sh
    $HADOOP_HOME/sbin/start-all.sh
    $HADOOP_HOME/sbin/stop-all.sh

#### Hadoop common syntax
    hdfs dfs -ls /
    hdfs dfs -mkdir /user/data/staging
    hdfs dfs -put $HIVE_HOME/examples/files/employee_part.txt /user/data/staging
    hdfs dfs -put $HIVE_HOME/examples/files/testHiveDriver.txt /user/data/staging
    hdfs dfs -put $HIVE_HOME/examples/files/person_partitioned.txt /user/data/staging



## SPARK
Start and stop the Spark Cluster

    $SPARK_HOME/sbin/start-all.sh
    $SPARK_HOME/sbin/stop-all.sh


## KAFKA
Start and stop the Kafka Cluster


## ZOOKEEPER
Start and stop the ZooKeeper

