CREATE SCHEMA ods;
use ods;
show tables;

--tbDate table creation
CREATE TABLE ods.tb_date(
    dataID string,
    theyearmonth string,
    theyear string,
    themonth string,
    thedate string,
    theweek string,
    theweeks string,
    thequot string,
    thetenday string,
    thehalfmonth string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


--tbStock table creation
CREATE TABLE ods.tb_stock(
    ordernumber STRING,
    locatitionid string,
    dataID string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


--tbStockDetail table creation
CREATE TABLE ods.tb_stock_detail(
    ordernumber STRING,
    rownum int,
    itemid string,
    qty int,
    price int,
    amout int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


--load data to Hive-warehouse
LOAD DATA LOCAL INPATH '/home/master/sales/tbDate.txt' INTO TABLE ods.tb_date;
LOAD DATA LOCAL INPATH '/home/master/sales/tbStock.txt' INTO TABLE ods.tb_stock;
LOAD DATA LOCAL INPATH '/home/master/sales/tbStockDetail.txt' INTO TABLE ods.tb_stock_detail;