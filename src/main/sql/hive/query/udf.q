-- export the HiveUDF Jar and add to Hive
add jar /home/master/sparkapps.jar;
CREATE TEMPORARY FUNCTION abc AS 'basics.HiveUDF';
SELECT abc(name) FROM worker;


--create a new rating table
DROP TABLE IF EXISTS ratings_new;
CREATE TABLE ratings_new (
    user_id INT,
    movie_id INT,
    rating INT,
    weekday INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


--add the udf python script
add FILE /home/master/weekday_mapper.py;
add FILE /home/master/string_mapper.py;


--execute the hive udf python script
INSERT OVERWRITE TABLE ratings_new
SELECT
TRANSFORM (user_id, movie_id, rating, ratings_timestamp)
          USING 'python3 weekday_mapper.py'
          AS (userid, movieid, rating, weekday)
FROM ratings
LIMIT  10;


SELECT TRANSFORM (name) USING 'python3 string_mapper.py' AS (name) FROM worker;

--select and check the results
SELECT weekday, COUNT(*)
FROM ratings_new
GROUP BY weekday;
