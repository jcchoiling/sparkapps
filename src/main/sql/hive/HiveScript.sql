-- execute hive -f HiveScript.sql
show databases;

use default;

show tables;

-- find a person whose salary has more than $2000
select name, sum(salary)
from worker
group by name
having sum(salary) > 2000;


