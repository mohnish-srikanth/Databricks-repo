-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Databricks notebook source
-- MAGIC def path_exists(path):
-- MAGIC   try:
-- MAGIC     dbutils.fs.ls(path)
-- MAGIC     return True
-- MAGIC   except Exception as e:
-- MAGIC     if 'java.io.FileNotFoundException' in str(e):
-- MAGIC       return False
-- MAGIC     else:
-- MAGIC       raise
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC def download_dataset(source, target):
-- MAGIC     files = dbutils.fs.ls(source)
-- MAGIC
-- MAGIC     for f in files:
-- MAGIC         source_path = f"{source}/{f.name}"
-- MAGIC         target_path = f"{target}/{f.name}"
-- MAGIC         if not path_exists(target_path):
-- MAGIC             print(f"Copying {f.name} ...")
-- MAGIC             dbutils.fs.cp(source_path, target_path, True)
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
-- MAGIC dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC def get_index(dir):
-- MAGIC     files = dbutils.fs.ls(dir)
-- MAGIC     index = 0
-- MAGIC     if files:
-- MAGIC         file = max(files).name
-- MAGIC         index = int(file.rsplit('.', maxsplit=1)[0])
-- MAGIC     return index+1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC # Structured Streaming
-- MAGIC streaming_dir = f"{dataset_bookstore}/orders-streaming"
-- MAGIC raw_dir = f"{dataset_bookstore}/orders-raw"
-- MAGIC
-- MAGIC def load_file(current_index):
-- MAGIC     latest_file = f"{str(current_index).zfill(2)}.parquet"
-- MAGIC     print(f"Loading {latest_file} file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")
-- MAGIC
-- MAGIC     
-- MAGIC def load_new_data(all=False):
-- MAGIC     index = get_index(raw_dir)
-- MAGIC     if index >= 10:
-- MAGIC         print("No more data to load\n")
-- MAGIC
-- MAGIC     elif all == True:
-- MAGIC         while index <= 10:
-- MAGIC             load_file(index)
-- MAGIC             index += 1
-- MAGIC     else:
-- MAGIC         load_file(index)
-- MAGIC         index += 1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC # DLT
-- MAGIC streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
-- MAGIC streaming_books_dir = f"{dataset_bookstore}/books-streaming"
-- MAGIC
-- MAGIC raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
-- MAGIC raw_books_dir = f"{dataset_bookstore}/books-cdc"
-- MAGIC
-- MAGIC def load_json_file(current_index):
-- MAGIC     latest_file = f"{str(current_index).zfill(2)}.json"
-- MAGIC     print(f"Loading {latest_file} orders file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
-- MAGIC     print(f"Loading {latest_file} books file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")
-- MAGIC
-- MAGIC     
-- MAGIC def load_new_json_data(all=False):
-- MAGIC     index = get_index(raw_orders_dir)
-- MAGIC     if index >= 10:
-- MAGIC         print("No more data to load\n")
-- MAGIC
-- MAGIC     elif all == True:
-- MAGIC         while index <= 10:
-- MAGIC             load_json_file(index)
-- MAGIC             index += 1
-- MAGIC     else:
-- MAGIC         load_json_file(index)
-- MAGIC         index += 1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC download_dataset(data_source_uri, dataset_bookstore)

-- COMMAND ----------

CREATE TABLE ORDERS_FROM_PARQUET AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM ORDERS_FROM_PARQUET;

-- COMMAND ----------

CREATE OR REPLACE TABLE ORDERS AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY ORDERS;

-- COMMAND ----------

INSERT OVERWRITE ORDERS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY ORDERS;

-- COMMAND ----------

INSERT OVERWRITE ORDERS
SELECT *,current_timestamp() FROM parquet.`${dataset.bookstore}/orders`;  --expected error

-- COMMAND ----------

INSERT INTO ORDERS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;  -- appending data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = 'true',
  delimiter = ';'
);

SELECT * FROM books_updates;

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN
 INSERT *

-- COMMAND ----------



-- COMMAND ----------


