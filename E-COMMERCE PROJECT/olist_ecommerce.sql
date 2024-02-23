create database Olist_kpi;
use olist_kpi;

create table  olist_customers_dataset (
	customer_id varchar(255),	
	customer_unique_id varchar(255),	
	customer_zip_code_prefix varchar(255),	
	customer_city varchar(255),	
	customer_state varchar(255)
);

create table  olist_geolocation_dataset (
    geolocation_zip_code_prefix varchar(255),
    geolocation_lat varchar(255),
    geolocation_lng varchar(25),
    geolocation_city varchar(255),
    geolocation_state varchar(255)
);

create table  olist_order_items_dataset (
   order_id	varchar(255),
   order_item_id varchar(255),
   product_id varchar(255),
   seller_id varchar(255), 
   shipping_limit_date varchar(255),
   price varchar(255),
   freight_value varchar(255)
);

create table olist_order_payments_dataset (
	order_id varchar(255),
	payment_sequential varchar(255),
	payment_type varchar(255),
	payment_installments varchar(255),
	payment_value varchar(255)
);

create table olist_order_reviews_dataset (
	review_id varchar(500),	
	order_id varchar(500),	
	review_score int,	
	review_comment_title varchar(255),	
	review_comment_message varchar(10000),	
	review_creation_date varchar(255),	
	review_answer_timestamp varchar(255)
);

create table olist_orders_dataset (
	order_id varchar(255),	
	customer_id varchar(255),	
	order_status varchar(255),	
	order_purchase_timestamp varchar(255),	
	order_approved_at varchar(255),	
	order_delivered_carrier_date varchar(255),	
	order_delivered_customer_date varchar(255),	
	order_estimated_delivery_date varchar(255)
);

create table olist_products_dataset (
	product_id varchar(255),	
	product_category_name varchar(255),	
	product_name_length varchar(255),	
	product_description_length varchar(255),	
	product_photos_qty varchar(255),	
	product_weight_g varchar(255),	
	product_length_cm varchar(255),	
	product_height_cm varchar(255),	
	product_width_cm varchar(255)
);

create table olist_sellers_dataset (
	seller_id varchar(255),	
	seller_zip_code_prefix varchar(255),	
	seller_city varchar(255),	
	seller_state varchar(255)
);


select @@secure_file_priv;

LOAD DATA INFILE 'C:\\olist_customers_dataset.csv' INTO TABLE olist_customers_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  LOAD DATA INFILE 'C:\\olist_geolocation_dataset.csv' INTO TABLE olist_geolocation_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  LOAD DATA INFILE 'C:\\olist_order_items_dataset.csv' INTO TABLE olist_order_items_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
ignore 1 lines;
 
 LOAD DATA INFILE 'C:\\olist_order_payments_dataset.csv' INTO TABLE olist_order_payments_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  LOAD DATA INFILE 'C:\\olist_order_reviews_dataset.csv' INTO TABLE olist_order_reviews_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;

LOAD DATA INFILE 'C:\\olist_orders_dataset.csv' INTO TABLE olist_orders_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
LOAD DATA INFILE 'C:\\olist_products_dataset.csv' INTO TABLE olist_products_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  load data infile 'C:\\olist_sellers_dataset.csv' INTO TABLE olist_sellers_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  select * from  olist_customers_dataset;
  select * from olist_geolocation_dataset;
  select * from olist_order_items_dataset;
  select * from olist_order_payments_dataset;
  select* from olist_order_reviews_dataset;
  select * from  olist_orders_dataset;
  select * from olist_products_dataset;
  select * from olist_sellers_dataset;
  
  ## KPI-I
## Weekday Vs Weekend (order_purchase_timestamp) Payment Statistics

select kpi1.day_end, round((kpi1.total_pmt/(select sum(payment_value) from 
olist_order_payments_dataset))*100,2) as perc_pmtvalue
from 
(select ord.day_end, sum(pmt.payment_value) as total_pmt
from olist_order_payments_Dataset as pmt join
(select distinct(order_id), case when weekday(order_purchase_timestamp) in (5,6) then "Weekend"
else "Weekday" end as Day_End from olist_orders_dataset) as ord on ord.order_id=pmt.order_id group by ord.day_end)
as kpi1;

## KPI-II
## Number of Orders with review score 5 and payment type as credit card.

select pmt.payment_type, count(pmt.order_id)
as total_orders from olist_order_payments_dataset as pmt join
(select distinct ord.order_id, rw.review_score from olist_orders_dataset as ord 
join olist_order_reviews_dataset rw on ord.order_id=rw.order_id where review_score=5) as rw5
on pmt.order_id=rw5.order_id group by pmt.payment_type order by total_orders desc;



## KPI-III
## Average number of days taken for order_delivered_customer_date for pet_shop

select prod.product_category_name,
round(avg(datediff(ord.order_delivered_customer_date,ord.order_purchase_timestamp)),0)
as avg_delivery_date
from olist_orders_dataset as ord join
(Select product_id,order_id,product_category_name from
olist_products_dataset join olist_order_items_dataset using (product_id)) as prod
on ord.order_id=prod.order_id where prod.product_category_name="pet_shop" group by prod.product_category_name;

## KPI-IV (A)
## Average price from customers of sao paulo city

Select cust.customer_city,round(avg(pmt_price.price),0) as avg_price
from olist_customers_dataset as cust
join (select pymnt.customer_id,pymnt.payment_value,item.price from olist_order_items_dataset as item join
(Select ord.order_id,ord.customer_id,pmt.payment_value from olist_orders_dataset as ord
join olist_order_payments_dataset as pmt on ord.order_id=pmt.order_id) as pymnt
on item.order_id=pymnt.order_id) as pmt_price on cust.customer_id=pmt_price.customer_id where cust.customer_city="sao paulo";

## KPI-IV (B)
## Payment values from customers of sao paulo city

Select cust.customer_city,round(avg(pmt.payment_value),0) as avg_payment_value 
from olist_customers_dataset cust inner join olist_orders_dataset ord 
on cust.customer_id=ord.customer_id inner join
olist_order_payments_dataset as pmt on ord.order_id=pmt.order_id 
where customer_city="sao paulo";

## KPI - V
## Relationship between shipping days (order_delivered_customer_date - order_purchase_timestamp) Vs review scores.

Select rw.review_score,
round(avg(datediff(ord.order_delivered_customer_date,ord.order_purchase_timestamp)),0) 
as avg_Shipping_Days
from olist_orders_dataset as ord join olist_order_reviews_dataset rw on 
rw.order_id=ord.order_id group by rw.review_score order by rw.review_score;
