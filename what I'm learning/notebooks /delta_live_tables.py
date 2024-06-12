# Databricks notebook source
# MAGIC %md
# MAGIC bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table orders_raw
# MAGIC comment "the raw books order, ingested from orders-raw"
# MAGIC as select * from cloud_files("dbfs:/mnt/demo-datasets/bookstore/orders-raw", "parquet", map("schema", "order_id string, order_timestamp long, customer_id string, quantity long"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh live table customers
# MAGIC comment "customers lookup table, ingested from customers-json"
# MAGIC as select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

# COMMAND ----------

# MAGIC %md
# MAGIC silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table orders_cleaned(
# MAGIC   constraint valid_order_number expect (order_id is not null) on violation drop row
# MAGIC )
# MAGIC comment "cleaned books with valid order_id"
# MAGIC as 
# MAGIC select order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp,
# MAGIC         c.profile:address:country as country
# MAGIC from stream(live.orders_raw) o  -- stream and live to access live stream delta tables
# MAGIC left join live.customers c    -- live to access live delta tables
# MAGIC on o.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC gold tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh live table cn_daily_customer_books
# MAGIC comment "daily number of books per customer in china"
# MAGIC as
# MAGIC select customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_count
# MAGIC from live.orders_cleaned
# MAGIC where country = 'China'
# MAGIC group by customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
