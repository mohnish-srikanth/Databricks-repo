-- Databricks notebook source
-- Create the customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    email STRING,
    profile STRING,
    updated DATE,
    PRIMARY KEY (customer_id)
);

-- Create the books table
CREATE TABLE IF NOT EXISTS books (
    book_id INT,
    title STRING,
    author STRING,
    category STRING,
    price DOUBLE,
    PRIMARY KEY (book_id)
);

-- Create the orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    timestamp TIMESTAMP,
    customer_id INT,
    quantity INT,
    total DOUBLE,
    book_id INT,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (book_id) REFERENCES books(book_id)
);

-- Insert sample data into customers table
INSERT INTO customers (customer_id, email, profile, updated) VALUES
(1, 'alice@example.com', 'Profile of Alice Johnson', '2023-05-01'),
(2, 'bob@example.com', 'Profile of Bob Smith', '2023-04-15'),
(3, 'carol@example.com', 'Profile of Carol White', '2023-03-20');

-- Insert sample data into books table
INSERT INTO books (book_id, title, author, category, price) VALUES
(1, 'The Great Gatsby', 'F. Scott Fitzgerald', 'Fiction', 10.99),
(2, '1984', 'George Orwell', 'Dystopian', 8.99),
(3, 'To Kill a Mockingbird', 'Harper Lee', 'Classic', 12.99);

-- Insert sample data into orders table
INSERT INTO orders (order_id, timestamp, customer_id, quantity, total, book_id) VALUES
(1, '2023-01-15 10:15:30', 1, 2, 21.98, 1),
(2, '2023-02-20 14:20:00', 2, 1, 8.99, 2),
(3, '2023-03-05 09:45:00', 3, 3, 38.97, 3);


-- COMMAND ----------

SELECT *
FROM customers;

-- COMMAND ----------

SELECT *
FROM orders;

-- COMMAND ----------

SELECT *
FROM books;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"/Volumes/demoworkspace/default/jsons_csvs")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("dbfs:/Volumes/demoworkspace/default/jsons_csvs/customers.json")
-- MAGIC df.createOrReplaceTempView("customers")
-- MAGIC df.show()
-- MAGIC #result = spark.sql("SELECT * FROM customers")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("dbfs:/Volumes/demoworkspace/default/jsons_csvs/books.json")
-- MAGIC df.createOrReplaceTempView("books")
-- MAGIC df.show()
-- MAGIC #result = spark.sql("SELECT * FROM books")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("dbfs:/Volumes/demoworkspace/default/jsons_csvs/orders.json")
-- MAGIC df.createOrReplaceTempView("orders")
-- MAGIC df.show()
-- MAGIC #result = spark.sql("SELECT * FROM orders")

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id INTEGER, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "dbfs:/Volumes/demoworkspace/default/jsons_csvs/books.csv",
  header = "true",
  delimiter = ";"
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table("books").write.mode("append").format("csv").option('header','true').option('delimiter',';').save("dbfs:/Volumes/demoworkspace/default/jsons_csvs"))
-- MAGIC # to read from one csv into another

-- COMMAND ----------

CREATE TABLE customers_from_json AS 
SELECT * FROM json.`dbfs:/Volumes/demoworkspace/default/jsons_csvs/customers.json`

-- COMMAND ----------

DESCRIBE EXTENDED customers_from_json;

-- COMMAND ----------

SELECT * FROM customers_from_json;

-- COMMAND ----------


