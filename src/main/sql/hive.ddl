/**Table Creation Script DDL**/
--Hive table creation: person
DROP TABLE IF EXISTS person;
CREATE TABLE person (id int, name String, age String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

--Hive table creation: worker
DROP TABLE IF EXISTS worker;
CREATE TABLE worker (id int, name String, salary Double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n';

--Load data into Hive
LOAD DATA LOCAL INPATH './examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;


