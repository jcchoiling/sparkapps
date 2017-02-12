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

SELECT * FROM worker;
SELECT COUNT(*) FROM worker;

SELECT name, salaries[2] FROM employee_array; --check the data in the array
SELECT name, salaries['3rd'] FROM employee_map; --check the data in the map
SELECT name, salaries.level FROM employee_struct; --check the data in the map
SELECT name, size(salaries)as salaries_length FROM employee_array;
SELECT * FROM employee_array WHERE array_contains(salaries,12000);

EXPLAIN SELECT * FROM worker;

FROM (
 SELECT name FROM worker where salary  > 2000
) e
SELECT e.name;

--DISTRIBUTE BY works similar to GROUP BY in the sense that it controls how reducers receive rows for processing,
--while SORT BY controls the sorting of data inside the reducer.

SELECT s.ymd, s.symbol, s.price_close FROM stocks s DISTRIBUTE BY s.symbol SORT BY s.symbol ASC, s.ymd ASC;
