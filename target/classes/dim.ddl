/*
Scenario: We are a E-commerce company, we will analyse the user behaviour using Spark.
The following tables have been created for this scenarios

Master Table
-- dim_date
-- dim_user
-- dim_item
-- dim_ads

Transaction Table
-- fct_item_transaction
-- rec_item_info


*/

/*******************************************************
* generic date table describes date, week and month
********************************************************/
CREATE TABLE dim_date (
	"id" INT NOT NULL,
	"month" DATE NOT NULL,
	"week" DATE NOT NULL,
	"date" DATE NOT NULL,
	PRIMARY KEY ("id")
);


/*******************************************************
* user table describes the basic information of users
********************************************************/
CREATE TABLE dim_user (
	"id" INT NOT NULL,
	"name" VARCHAR(20) NOT NULL,
	"gender" VARCHAR(1) NOT NULL,
	PRIMARY KEY ("id")
);


/*******************************************************
* item table describes the item category, name and price
********************************************************/
CREATE TABLE dim_item (
	"id" INT NOT NULL,
	"item_category" VARCHAR(50) NOT NULL,
	"item_name" VARCHAR(50) NOT NULL,
	"item_price" REAL,
	PRIMARY KEY ("id")
);

/*******************************************************
* ads table describes the ads name and ads price
********************************************************/
CREATE TABLE dim_ads (
	"id" INT NOT NULL,
	"ads_name" VARCHAR(50) NOT NULL,
	"ads_price" REAL,
	PRIMARY KEY ("id")
);

/*******************************************************
* blacklist table describes the blacklisted user
********************************************************/
CREATE TABLE dim_blacklist (
	"id" INT NOT NULL,
	"user_id" INT NOT NULL,
	PRIMARY KEY ("id")
);


/*******************************************************
* transaction table describes item transaction per user
********************************************************/
CREATE TABLE fct_item_transaction (
	"id" INT NOT NULL,
	"user_id" INT NOT NULL,
	"item_id" INT NOT NULL,
	"qty" INT,
	"unit_price" REAL,
	"total_price" REAL,
	PRIMARY KEY ("id")
);

/*******************************************************
* Receommendation table
********************************************************/
	CREATE TABLE rec_item_info (
	"id" INT NOT NULL,
	"user_id" INT NOT NULL,
	"item_id" INT NOT NULL,
	"qty" INT,
	"unit_price" REAL,
	"total_price" REAL,
	PRIMARY KEY ("id")
);


CREATE DATABASE ott;

DROP TABLE IF EXISTS person;
CREATE TABLE person (
	name varchar(20),
	age int
);

INSERT INTO person (name, age) VALUES
('Alex', 20),
('Janice', 28),
('Peter', 32)
;







