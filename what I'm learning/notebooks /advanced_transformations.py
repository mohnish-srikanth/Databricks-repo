# Databricks notebook source
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

# MAGIC %sql
# MAGIC CREATE TABLE ORDERS_FROM_PARQUET AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ORDERS_FROM_PARQUET

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE ORDERS_FROM_PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT books[0].book_id
# MAGIC FROM ORDERS_FROM_PARQUET
# MAGIC WHERE ORDER_ID = 000000000003825

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_json(books[0])
# MAGIC FROM ORDERS_FROM_PARQUET  -- BOUND TO FAIL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT books[0]
# MAGIC FROM ORDERS_FROM_PARQUET
# MAGIC WHERE ORDER_ID = 000000000003825

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_json_parsed AS 
# MAGIC   SELECT order_id, customer_id, from_json(books[0], schema_of_json('{"book_id":"B09","quantity":2,"subtotal":48}')) as order_from_json
# MAGIC   FROM ORDERS_FROM_PARQUET
# MAGIC   WHERE ORDER_ID = 000000000003825;
# MAGIC
# MAGIC SELECT * FROM orders_json_parsed;   -- this did not work for me becuase the json value was already in struct type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, books[0]
# MAGIC FROM ORDERS_FROM_PARQUET
# MAGIC WHERE ORDER_ID = 000000000003825;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW seperate_columns_from_struct_col AS
# MAGIC   SELECT order_id, books[0]
# MAGIC   FROM ORDERS_FROM_PARQUET
# MAGIC   WHERE ORDER_ID = 000000000003825;
# MAGIC
# MAGIC SELECT * FROM seperate_columns_from_struct_col;  -- ideally creates a new row for each key in the json

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ORDERS_FROM_PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, explode(books) as book
# MAGIC FROM ORDERS_FROM_PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC   collect_set(order_id) AS orders_set,
# MAGIC   collect_set(books.book_id) as books_set
# MAGIC from ORDERS_FROM_PARQUET
# MAGIC group by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id,
# MAGIC   collect_set(books.book_id) as before_flatten,
# MAGIC   array_distinct(flatten(collect_set(books.book_id))) as after_flatten
# MAGIC from ORDERS_FROM_PARQUET
# MAGIC group by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_from_parquet AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/books`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table transactions as 
# MAGIC
# MAGIC select * from (
# MAGIC   select customer_id,
# MAGIC   book.book_id as book_id,
# MAGIC   book.quantity as quantity
# MAGIC   from ORDERS_FROM_PARQUET
# MAGIC ) pivot (
# MAGIC sum(quantity) for book_id in (
# MAGIC   'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
# MAGIC )
# MAGIC );
# MAGIC
# MAGIC select * from transactions
