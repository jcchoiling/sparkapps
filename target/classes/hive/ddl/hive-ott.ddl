---------------------------------------------
--Create Hive Schmea for keeping the movie data
---------------------------------------------
CREATE SCHEMA ott;
use ott;
show tables;

---------------------------------------------
--Movie information: MovieID::Title::Genres
---------------------------------------------
DROP TABLE if EXISTS ott.movies;
CREATE TABLE movies (
    movie_id int COMMENT 'unique movie id for moviews table',
    title string COMMENT 'movie title for movie table',
    genres string COMMENT 'movie genres'
)
COMMENT 'This is the movie data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

--建立索引一般都会有 partition
CREATE INDEX index_movies ON TABLE movies(movie_id)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD ;

---------------------------------------------
--Age information: AgeID::AgeRange
---------------------------------------------
DROP TABLE if EXISTS ott.age;
CREATE TABLE age (
    age_id int COMMENT 'unique age id for age table',
    age_range string COMMENT 'age range such as 18-25'
)
COMMENT 'This is the age data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE INDEX index_age ON TABLE age(age_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD ;

---------------------------------------------------------------------
--User information: UserID::Gender::AgeID::OccupationID::Zip-code
---------------------------------------------------------------------
DROP TABLE if EXISTS ott.users;
CREATE TABLE users (
    user_id int COMMENT 'unique user id for users table',
    gender string COMMENT 'user gender',
    age_id int COMMENT 'age id foreign key for age table',
    occupation_id int COMMENT 'occupation id foreign key for age table',
    zip_code int COMMENT 'user zip code'
)
COMMENT 'This is the user data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE INDEX index_users ON TABLE users(user_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD ;

-----------------------------------------------------
-- Occupation information: OccupationID::Occupation
-----------------------------------------------------
DROP TABLE if EXISTS ott.occupations;
CREATE TABLE occupations (
    occupation_id int COMMENT 'unique occupation id for occupations table',
    occupation_name string  COMMENT 'occupation name for occupations table'
)
COMMENT 'This is the occupation data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE INDEX index_occupations ON TABLE occupations(occupation_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD ;

-----------------------------------------------------------------------------------
-- Rating information: UserID::MovieID::Rating::Timestamp (1::1193::5::978300760)
-----------------------------------------------------------------------------------
DROP TABLE if EXISTS ott.ratings;
CREATE TABLE ratings (
    user_id int COMMENT 'user id foreign key for users table',
    movie_id int COMMENT 'movie id foreign key for movies table',
    rating int COMMENT 'rating given by user per movie',
    ratings_timestamp int COMMENT 'record insert timestamp'
)
COMMENT 'This is the rating data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE INDEX index_ratings ON TABLE ratings(user_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD ;



-----------------------------------------------------------
-- Importing local data to Hive
-----------------------------------------------------------
LOAD DATA LOCAL INPATH '/home/master/moviedata/movies.dat' OVERWRITE INTO TABLE movies;
LOAD DATA LOCAL INPATH '/home/master/moviedata/ratings.dat' OVERWRITE INTO TABLE ratings;
LOAD DATA LOCAL INPATH '/home/master/moviedata/occupations.dat' OVERWRITE INTO TABLE occupations;
LOAD DATA LOCAL INPATH '/home/master/moviedata/users.dat' OVERWRITE INTO TABLE users;
LOAD DATA LOCAL INPATH '/home/master/moviedata/age.dat' OVERWRITE INTO TABLE age;

ALTER INDEX index_ratings ON ratings rebuild;


