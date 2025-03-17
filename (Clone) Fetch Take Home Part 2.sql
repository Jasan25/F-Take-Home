-- Databricks notebook source
create or replace table new_transaction as
select * from transaction where FINAL_QUANTITY != 'zero' and FINAL_SALE is not null

-- COMMAND ----------


-- What are the top 5 brands by receipts scanned among users 21 and over?
select p.brand, 
-- t.RECEIPT_ID, PURCHASE_DATE, BIRTH_DATE
count(receipt_id) as receipt_by_brand
from products p 
join new_transaction t on p.BARCODE = t.BARCODE
join user u on t.USER_ID = u.ID
where 1= 1 
and date_diff(t.PURCHASE_DATE, date(u.BIRTH_DATE))/ 365 >= 21 
and p.brand is not null
group by 1
order by 2 desc
limit 5

-- COMMAND ----------


-- What are the top 5 brands by sales among users that have had their account for at least six months?
select p.brand, 
sum(t.FINAL_SALE) as sale_amt
from products p 
join new_transaction t on p.BARCODE = t.BARCODE
join user u on t.USER_ID = u.ID
where 1= 1 
and date_diff(t.PURCHASE_DATE, date(u.created_date))/ 30 >= 6
and p.brand is not null
group by 1
order by 2 desc
limit 5

-- COMMAND ----------


-- What is the percentage of sales in the Health & Wellness category by generation?
with category_sales as ( 
  select p.category_1, t.user_id, t.PURCHASE_DATE,
  sum(t.FINAL_SALE) as sale_amt
  from products p 
  join new_transaction t on p.BARCODE = t.BARCODE
  group by 1,2,3
)
,
user_info as (
  select cat.*,
  round(date_diff(PURCHASE_DATE,date(BIRTH_DATE)) / 365,1) as age
  from category_sales cat
  join user u on cat.user_id = u.id
)
,
age_group as (
  select category_1,
  case when age <= 0 then '0 - Special'
      when age > 0 and age < 10 then '1 - Under 10'
      when age >= 10 and age < 30 then '2 - 10-29'
      when age >= 30 and age < 50 then '3 - 30-49'
      when age >= 50 and age < 70 then '4 - 50-69'
      when age >= 70 and age < 90 then '5 - 70-89'
      else '6 - 90+'
      end as Age_grp,
      sum(sale_amt) as total_sale_amt
  from user_info
  group by 1,2
)

select category_1, age_grp, round(total_sale_amt/ total_sale_amt_by_age,2) * 100 as percentage_of_sales
from (
  select category_1, age_grp, total_sale_amt, sum(total_sale_amt) over (partition by age_grp) as total_sale_amt_by_age
  from age_group
)
where category_1 = 'Health & Wellness'

-- COMMAND ----------

-- Who are Fetch’s power users?
-- Assumption: Power users are those who have made more than 10 purchases in the transaction table

-- COMMAND ----------

select user_id, count(receipt_id) as num_receipts from new_transaction 
group by user_id
having count(receipt_id) >= 10
order by num_receipts desc
-- however these user_id doesn't exist in the user table which we can't get more info

-- COMMAND ----------

--Which is the leading brand in the Dips & Salsa category?
-- Assumption: Dips & Salsa is the category_2 column and leading brand is defined as more than or equal to 100 barcodes 

-- COMMAND ----------

select brand, count(BARCODE) as num_barcode from products
where category_2 = 'Dips & Salsa' and brand is not null
group by 1
having num_barcode >= 100
order by 2 desc

-- COMMAND ----------

--At what percent has Fetch grown year over year?
-- since the data is not available in the transaction table, we would assume the # of users in the user table as the growth

-- COMMAND ----------

select join_year, round((sign_up - prev_year_sign_up)/prev_year_sign_up,2) * 100 as growth_pct
from (
  select join_year, lag(sign_up) over (order by join_year) as prev_year_sign_up, sign_up
  from (
    select year(CREATED_DATE) as join_year, count(ID) AS Sign_up from user
    group by 1
  )
)
