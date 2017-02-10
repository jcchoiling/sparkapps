--每年每季度销售排名前十名的商品
SELECT
t1.ordernumber,
sum(t2.amount) as total_fees
FROM ods.tb_stock as t1
     JOIN ods.tb_stock_detail as t2 ON t1.ordernumber = t2.ordernumber
GROUP BY t1.ordernumber
HAVING total_fees > 50000;


--所有的订单系统每年最畅销的产品
SELECT
    t.theyear,
    t.ordernumber,
    t.total_amount,
    t.rank
FROM
(
SELECT
t3.theyear,
t1.ordernumber,
sum(t1.amount) as total_amount,
ROW_NUMBER() OVER(PARTITION BY t3.theyear
                  ORDER BY sum(t1.amount) DESC) as rank
FROM ods.tb_stock_detail as t1
     JOIN ods.tb_stock as t2 ON t1.ordernumber = t2.ordernumber
     JOIN ods.tb_date as t3 ON t2.dataID = t3.dataID
GROUP BY t3.theyear, t1.ordernumber
)t
WHERE t.rank < 5
ORDER BY t.theyear, t.rank;



--所有的订单系统每年最畅销的产品
SELECT
    c.theyear,
    c.thequot,
    sum(b.amount) AS sumofamount
FROM ods.tb_stock a,
JOIN ods.tb_stock_detail b ON a.ordernumber=b.ordernumber
JOIN ods.tb_date c ON a.dataid=c.dataid
GROUP BY c.theyear, c.thequot
ORDER BY sumofamount
DESC LIMIT 10;


SELECT
t3.theyear,
t3.thequot,
sum(t1.amount) as total_fees,
ROW_NUMBER() OVER(PARTITION BY t3.theyear
                  ORDER BY sum(t1.amount) DESC) as rank
FROM ods.tb_stock_detail as t1
     JOIN ods.tb_stock as t2 ON t1.ordernumber = t2.ordernumber
     JOIN ods.tb_date as t3 ON t2.dataID = t3.dataID
GROUP BY t3.theyear, t3.thequot;