# Databricks notebook source
files = dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/orders-raw')
display(files)

# COMMAND ----------

spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint").load(f"dbfs:/mnt/demo-datasets/bookstore/orders-raw").writeStream.option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint").table("orders_updates")  
# auto loader uses read and write stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates

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

load_new_data()  # ran twice

# COMMAND ----------

files = dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/orders-raw')
display(files)  # 2 new added files can be seen

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates  
# MAGIC -- shows 2k more count
# MAGIC -- if auto loaded is running, this table will be updated automatically

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders_updates  -- shows everytime the data was updated by running auto loader

# COMMAND ----------


