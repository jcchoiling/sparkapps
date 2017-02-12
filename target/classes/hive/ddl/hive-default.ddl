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

--Hive table creation: employee_array
DROP TABLE IF EXISTS employee_array;
CREATE TABLE employee_array (
    id int,
    name String,
    address String,
    salaries array<int>,
    gender String
)
COMMENT 'This is the employee_array table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
COLLECTION ITEMS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

--Hive table creation: employee_map
DROP TABLE IF EXISTS employee_map;
CREATE TABLE employee_map (
    id int,
    name String,
    address String,
    salaries MAP<String, BIGINT>,
    gender String
)
COMMENT 'This is the employee_map table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

--Hive table creation: employee_struct
DROP TABLE IF EXISTS employee_struct;
CREATE TABLE employee_struct (
    id int,
    name String,
    address String,
    salaries struct<s1:bigint, s2:bigint, s3:bigint, level:string>,
    gender String
)
COMMENT 'This is the employee_struct table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
COLLECTION ITEMS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- programming hive examples
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    name STRING,
    salary FLOAT,
    subordinates ARRAY<STRING>,
    deductions MAP<STRING,FLOAT>,
    address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
PARTITIONED BY (country STRING, state STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


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
LOCATION '/user/data/staging/employee';

--Hive table creation: person with partition
--Hive 的最住实战：采用 partitioned 分区表 + Parquet 文件存储的方式！
--Hive 的最住实战：一般情况下分区都是按时间去分区的！

DROP TABLE IF EXISTS employee_partitioned;
CREATE EXTERNAL TABLE employee_partitioned (id int, name String, salary Double, country String)
COMMENT 'This is the staging page view table'
PARTITIONED BY (pdate String, phour String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/data/staging/employee_partitioned';


DROP TABLE IF EXISTS employee_rc;
CREATE EXTERNAL TABLE employee_rc (id int, name String, salary Double, country String)
COMMENT 'This is the staging page view table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS RCFILE
LOCATION '/user/data/staging/employee_rc';

--Bucketing
CREATE EXTERNAL TABLE IF NOT EXISTS stocks (
exchange STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT
)
CLUSTERED BY (exchange, symbol)
SORTED BY (ymd ASC)
INTO 96 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/stocks';


-- mutiple insert
FROM staged_employees se
    INSERT OVERWRITE TABLE employees PARTITION (country = 'US', state = 'OR')
    SELECT * WHERE se.cnty = 'US' AND se.st = 'OR'
    INSERT OVERWRITE TABLE employees PARTITION (country = 'US', state = 'CA')
    SELECT * WHERE se.cnty = 'US' AND se.st = 'CA'
    INSERT OVERWRITE TABLE employees PARTITION (country = 'US', state = 'IL')
    SELECT * WHERE se.cnty = 'US' AND se.st = 'IL';

-- dynamic partitioning
INSERT OVERWRITE TABLE employees PARTITION (country, state)
    SELECT se.name, se.cnty, se.st
    FROM staged_employees se;

--Export the file to the local directory
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/ca_employees'
SELECT name, salary, address
FROM employees
WHERE se.state = 'CA';


CREATE EXTERNAL TABLE dynamictable(
cols map<string,string>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\004'
COLLECTION ITEMS TERMINATED BY '\001'
MAP KEYS TERMINATED BY '\002'
STORED AS TEXTFILE;

CREATE VIEW orders(state, city, part) AS
SELECT cols["state"], cols["city"], cols["part"]
FROM dynamictable WHERE cols["type"] = "request";

--------------------------------------------
--adding partition to the table
--------------------------------------------
ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION(pdate='20170101', phour='1200');
ALTER TABLE employee_partitioned ADD PARTITION (pdate='20170101', phour='1200');
SHOW PARTITIONS employee_partitioned;




--------------------------------------------
--Load data into Hive
--------------------------------------------
--Load data from local
LOAD DATA LOCAL INPATH './examples/files/employee_part.txt' OVERWRITE INTO TABLE worker;
--Query res: Loading data to table default.worker

--Load data from local
INSERT INTO employee_rc SELECT * FROM worker;

--Load data from hdfs
LOAD DATA INPATH '/user/data/staging/employee_part.txt' OVERWRITE INTO TABLE employee;

--Load data from hdfs
LOAD DATA INPATH '/user/data/staging/employee_part/employee_part.txt'
OVERWRITE INTO TABLE employee_partitioned
PARTITION(pdate='20170101', phour='1200');

--Load data from hdfs
LOAD DATA LOCAL INPATH './examples/files/employee_array.txt' OVERWRITE INTO TABLE employee_array;
LOAD DATA LOCAL INPATH './examples/files/employee_map.txt' OVERWRITE INTO TABLE employee_map;
LOAD DATA LOCAL INPATH './examples/files/employee_struct.txt' OVERWRITE INTO TABLE employee_struct;

LOAD DATA LOCAL INPATH './examples/files/shipment.txt' OVERWRITE INTO TABLE dynamictable;








