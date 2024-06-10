# Databricks notebook source
files = dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/orders-raw')
display(files)

# COMMAND ----------

spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint").load(f"dbfs:/mnt/demo-datasets/bookstore/orders-raw").createOrReplaceTempView("orders_raw_temp")  # creating a data stream to orders_raw_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view orders_temp as(
# MAGIC   select *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   from orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_temp

# COMMAND ----------

spark.table("orders_temp").writeStream.format("delta").option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze").outputMode("append").table("orders_bronze")  
# bronze layer delta table created using writeSteam

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

# Databricks notebook source
def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def download_dataset(source, target):
    files = dbutils.fs.ls(source)

    for f in files:
        source_path = f"{source}/{f.name}"
        target_path = f"{target}/{f.name}"
        if not path_exists(target_path):
            print(f"Copying {f.name} ...")
            dbutils.fs.cp(source_path, target_path, True)

# COMMAND ----------

data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_bookstore}/orders-streaming"
raw_dir = f"{dataset_bookstore}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# DLT
streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
streaming_books_dir = f"{dataset_bookstore}/books-streaming"

raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
raw_books_dir = f"{dataset_bookstore}/books-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

download_dataset(data_source_uri, dataset_bookstore)

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

spark.read.format("json").load(f"dbfs:/mnt/demo-datasets/bookstore/customers-json").createOrReplaceTempView("customers_lookup")  
# loading customer data into a lookup view to initialize silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup

# COMMAND ----------

spark.readStream.table("orders_bronze").createOrReplaceTempView("orders_bronze_temp")   
# creating temp view against bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view orders_enriched_temp as(
# MAGIC   select order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC   cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp, books
# MAGIC   from orders_bronze_temp o
# MAGIC   inner join customers_lookup c
# MAGIC   on o.customer_id = c.customer_id
# MAGIC   where quantity > 0
# MAGIC )   -- creating silver level table for more info

# COMMAND ----------

spark.table("orders_enriched_temp").writeStream.format("delta").option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver").outputMode("append").table("orders_silver")   # creating a writeStream to load refined data into silver tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver

# COMMAND ----------

load_new_data() # newly loaded data will propagate from bronze to silver layer

# COMMAND ----------

spark.readStream.table("orders_silver").createOrReplaceTempView("orders_silver_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view daily_cutomer_books_temp as(
# MAGIC   select customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_data, sum(quantity) books_count
# MAGIC   from orders_silver_temp
# MAGIC   group by customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

spark.table("daily_cutomer_books_temp").writeStream.format("delta").outputMode("complete").option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_gold").trigger(availableNow=True).table("daily_customer_books")  # writeStream to a gold table daily_customer_books
# this stream will stop and not run continuously as we are using availableNow option

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books

# COMMAND ----------

load_new_data(all = True) 
# this will load files that flow through bronze and silver layer automatically since these layers user a continuous stream
# need to retrigger only gold layer since that was a batch job

# COMMAND ----------

for s in spark.streams.active:
    print("stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
# this loop will terminate all actively running streams

# COMMAND ----------


