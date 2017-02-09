--------------------------------------------
--Table Creation Script DDL
--------------------------------------------

--Hive table creation: person
DROP TABLE IF EXISTS person;
CREATE TABLE person (id int, name String, age int)
COMMENT 'This is the person table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

--Hive table creation: worker
DROP TABLE IF EXISTS worker;
CREATE TABLE worker (id int, name String, salary Double, country String)
COMMENT 'This is the worker table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n';


--Hive table creation: person with partition
--Hive 的最住实战：采用 partitioned 分区表 + Parquet 文件存储的方式！
--Hive 的最住实战：一般情况下分区都是按时间去分区的！
DROP TABLE IF EXISTS employee_partitioned;
CREATE EXTERNAL TABLE employee (id int, name String, salary Double, country String)
COMMENT 'This is the staging page view table'
PARTITIONED BY (pdate String, phour String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/data/staging/employee_part';


DROP TABLE IF EXISTS employee_partitioned;
CREATE EXTERNAL TABLE employee (id int, name String, salary Double, country String)
COMMENT 'This is the staging page view table'
PARTITIONED BY (pdate String, phour String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/data/staging/employee_part';


--------------------------------------------
--adding partition to the table
--------------------------------------------
ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION(pdate='20170101', phour='1200');
ALTER TABLE employee_partitioned ADD PARTITION (pdate='20170101', phour='1200');
SHOW PARTITIONS employee_partitioned;



--------------------------------------------
--Create Hive external table creation: employee
--------------------------------------------
--Hive external table creation: employee
DROP TABLE IF EXISTS employee;
CREATE EXTERNAL TABLE employee (id int, name String, salary Double, country String)
COMMENT 'This is the staging page view table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/data/staging/employee_part';




--------------------------------------------
--Load data into Hive
--------------------------------------------
--Load data from local
LOAD DATA LOCAL INPATH './examples/files/employee_part.txt'
OVERWRITE INTO TABLE worker;
--Query res: Loading data to table default.worker

--Load data from hdfs
LOAD DATA INPATH '/user/data/staging/employee_part.txt'
OVERWRITE INTO TABLE employee;

--Load data from hdfs
LOAD DATA INPATH '/user/data/staging/employee_part/employee_part.txt'
OVERWRITE INTO TABLE employee_partitioned
PARTITION(pdate='20170101', phour='1200');








