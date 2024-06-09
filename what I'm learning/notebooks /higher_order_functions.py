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

dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore')

# COMMAND ----------

dbutils.fs.ls('wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_from_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers_from_json as
# MAGIC select * from json.`wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/customers-json/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_from_json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_from_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books_from_csv as
# MAGIC select * from csv.`wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/books-csv/`

# COMMAND ----------

dbutils.fs.ls("wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/books-csv/")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table temp_books_from_csv
# MAGIC using csv
# MAGIC options(path "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/books-csv/export_004.csv", headers "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_from_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table books_from_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books_from_csv
# MAGIC using csv
# MAGIC options(
# MAGIC   path =  'wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/books-csv/',
# MAGIC   header = 'True',
# MAGIC   mode = 'FAILFAST'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, books, filter(books, i -> i.quantity >= 2) as mutilplecopies
# MAGIC from orders_from_parquet   -- filter function

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from (SELECT order_id, books, filter(books, i -> i.quantity >= 2) as mutilplecopies
# MAGIC from orders_from_parquet )
# MAGIC where size(mutilplecopies) >= 1   -- size function

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, books, books.subtotal, transform(books, b -> cast(b.subtotal * 0.8 as int)) as subtotal_after_discount
# MAGIC from orders_from_parquet  --transform and cast function used to apply discount7

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function get_url(email string)   -- function defined here 
# MAGIC
# MAGIC returns string
# MAGIC return concat("https://www.", split(email, "@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC select email, get_url(email) domain  -- defined function used here 
# MAGIC from customers_from_json

# COMMAND ----------

# MAGIC %sql
# MAGIC describe function extended get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC create function site_type(email string)
# MAGIC returns string
# MAGIC return case
# MAGIC           when email like "%.com" then "commercial business"
# MAGIC           when email like "%.org" then "ngo"
# MAGIC           when email like "%.edu" then "educational institution"
# MAGIC           else concat("unknown extention for domain:", split(email, "@")[1])
# MAGIC         end
# MAGIC         

# COMMAND ----------

# MAGIC %sql
# MAGIC select email, site_type(email)
# MAGIC from customers_from_json

# COMMAND ----------

# MAGIC %sql
# MAGIC describe function extended site_type
