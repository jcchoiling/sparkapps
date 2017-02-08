--------------------------------------------
--Table Creation Script DDL
--------------------------------------------

--Hive table creation: person
DROP TABLE IF EXISTS person;
CREATE TABLE person (id int, name String, age String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

--Hive table creation: worker
DROP TABLE IF EXISTS worker;
CREATE TABLE worker (id int, name String, salary Double, country String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n';


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
--Loading data to table default.employee, after loading data from hdfs, the data file in the hdfs will disappeared, becuase the file has already been loaded into Hive









