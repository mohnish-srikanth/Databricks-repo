# Databricks notebook source
spark.readStream.table("orders_from_parquet").createOrReplaceTempView("orders_streaming_tmp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_streaming_tmp_vw;  -- live feed of the steam

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, count(order_id)
# MAGIC from orders_streaming_tmp_vw
# MAGIC group by customer_id  -- this is also an active steaming query, data is not persisted anywhere for this

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_streaming_tmp_vw order by quantity  -- error as streaming does not support ordering 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view order_count_tmp_vw as(
# MAGIC   select customer_id, count(order_id) as count_of_orders
# MAGIC   from orders_streaming_tmp_vw
# MAGIC   group by customer_id
# MAGIC )   -- this created streaming temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_count_tmp_vw

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table("order_count_tmp_vw")
df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])  # tried to replace any invalid charecters in column name but did it by changing the table 

# COMMAND ----------

spark.table("order_count_tmp_vw").writeStream.trigger(processingTime='4 seconds').outputMode("complete").option("checkpointLocation", "dbfs:/mnt/demo/order_count_checkpoint").table("order_counts")  # craetes a table that persists data from the stream table every 4 seconds data is updated in new table
# this will keep running as mew data getting added to source table will keep updating new table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_from_parquet (
# MAGIC     order_id, 
# MAGIC     order_timestamp, 
# MAGIC     customer_id, 
# MAGIC     quantity, 
# MAGIC     total, 
# MAGIC     books
# MAGIC ) VALUES
# MAGIC (
# MAGIC     '000000000003491',
# MAGIC     1657512356,
# MAGIC     'C00020',
# MAGIC     2,
# MAGIC     40,
# MAGIC     ARRAY(STRUCT('B04', 2, 40))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003492',
# MAGIC     1657512456,
# MAGIC     'C00021',
# MAGIC     1,
# MAGIC     20,
# MAGIC     ARRAY(STRUCT('B05', 1, 20))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003493',
# MAGIC     1657512556,
# MAGIC     'C00022',
# MAGIC     3,
# MAGIC     60,
# MAGIC     ARRAY(STRUCT('B06', 3, 60))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003494',
# MAGIC     1657512656,
# MAGIC     'C00023',
# MAGIC     4,
# MAGIC     80,
# MAGIC     ARRAY(STRUCT('B07', 4, 80))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003495',
# MAGIC     1657512756,
# MAGIC     'C00024',
# MAGIC     5,
# MAGIC     100,
# MAGIC     ARRAY(STRUCT('B08', 5, 100))
# MAGIC );
# MAGIC
# MAGIC -- adding some more data and restarting the enitre streaming process

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_from_parquet (
# MAGIC     order_id, 
# MAGIC     order_timestamp, 
# MAGIC     customer_id, 
# MAGIC     quantity, 
# MAGIC     total, 
# MAGIC     books
# MAGIC ) VALUES
# MAGIC (
# MAGIC     '000000000003496',
# MAGIC     1657512856,
# MAGIC     'C00025',
# MAGIC     2,
# MAGIC     50,
# MAGIC     ARRAY(STRUCT('B09', 1, 25), STRUCT('B10', 1, 25))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003497',
# MAGIC     1657512956,
# MAGIC     'C00026',
# MAGIC     1,
# MAGIC     30,
# MAGIC     ARRAY(STRUCT('B11', 1, 30))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003498',
# MAGIC     1657513056,
# MAGIC     'C00027',
# MAGIC     3,
# MAGIC     75,
# MAGIC     ARRAY(STRUCT('B12', 2, 50), STRUCT('B13', 1, 25))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003499',
# MAGIC     1657513156,
# MAGIC     'C00028',
# MAGIC     4,
# MAGIC     120,
# MAGIC     ARRAY(STRUCT('B14', 3, 90), STRUCT('B15', 1, 30))
# MAGIC ),
# MAGIC (
# MAGIC     '000000000003500',
# MAGIC     1657513256,
# MAGIC     'C00029',
# MAGIC     5,
# MAGIC     150,
# MAGIC     ARRAY(STRUCT('B16', 5, 150))
# MAGIC );
# MAGIC

# COMMAND ----------

spark.table("order_count_tmp_vw").writeStream.trigger(availableNow=True).outputMode("complete").option("checkpointLocation", "dbfs:/mnt/demo/orders_count_checkpoint").table("orders_count").awaitTermination  # this will bring only new data inserted into the stream
# this stream does not run all the time since it is one time load of new data only, batch mode

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_count

# COMMAND ----------


