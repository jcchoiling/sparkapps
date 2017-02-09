-- execute hive -f HiveScript.sql
SHOW DATABASES;

USE default;

SHOW TABLES;

SHOW INDEX ON ratings;

SHOW PARTITIONS ott.ratings;

-- find a person whose salary has more than $2000
SELECT name, sum(salary)
FROM worker
GROUP BY name
HAVING sum(salary) > 2000;


-- Look for the top10 titles which has the most ratings
-- Time taken: 52.883 seconds, Fetched: 438 row(s)
SELECT
users.user_id,
users.gender,
age.age_range
FROM ratings
     JOIN users on ratings.user_id = users.user_id
     JOIN age on users.age_id = age.age_id
WHERE ratings.movie_id = 2116;


-- Look for the top10 titles which has the most ratings
-- Time taken: 21.452 seconds, Fetched: 438 row(s)
-- You can configure Map Join and share the map reduce job to mapper side.
SELECT
/*+MAPJOIN(movies)*/
users.user_id,
users.gender,
age.age_range,
movies.title,
movies.genres
FROM ratings
     JOIN users on ratings.user_id = users.user_id
     JOIN age on users.age_id = age.age_id
     JOIN movies on ratings.movie_id = movies.movie_id
WHERE ratings.movie_id = 2116;


-- Look for the top10 titles which has the most ratings
SELECT
movies.title,
movies.genres,
sum(ratings.rating) as total_rating
FROM ratings
     JOIN movies on ratings.movie_id = movies.movie_id
GROUP BY movies.title, movies.genres
ORDER BY total_rating desc
LIMIT 10;
