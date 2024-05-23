# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.default.employees
# MAGIC   (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO hive_metastore.default.employees (id, name, salary) VALUES
# MAGIC (1, 'Alice Johnson', 55000),
# MAGIC (2, 'Bob Smith', 60000),
# MAGIC (3, 'Carol White', 62000),
# MAGIC (4, 'David Brown', 58000),
# MAGIC (5, 'Eve Davis', 59000),
# MAGIC (6, 'Frank Wilson', 57000),
# MAGIC (7, 'Grace Lee', 61000);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.default.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL hive_metastore.default.employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE hive_metastore.default.employees
# MAGIC SET salary = salary + 100
# MAGIC WHERE name like 'A%';

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL hive_metastore.default.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hive_metastore.default.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hive_metastore.default.employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'
