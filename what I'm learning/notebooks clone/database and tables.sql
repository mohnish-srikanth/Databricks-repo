-- Databricks notebook source
CREATE TABLE hive_metastore.default.managed_default
  (width INT, length INT, height INT);

-- COMMAND ----------

INSERT INTO hive_metastore.default.managed_default
VALUES(3 INT, 2 INT, 1 INT)

-- COMMAND ----------

SELECT * FROM hive_metastore.default.managed_default;

-- COMMAND ----------

DROP TABLE employees;

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.default.managed_default;

-- COMMAND ----------

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/demoworkspace/default/external_default';

INSERT INTO external_default
VALUES(3 INT, 2 INT, 1 INT);

-- COMMAND ----------

CREATE SCHEMA external_schema;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED external_schema;

-- COMMAND ----------

USE external_schema;
CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/demoworkspace/external_schema/external_default';

INSERT INTO external_default
VALUES(3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.default.employees;

-- COMMAND ----------

DESCRIBE EXTENDED demoworkspace.default.managed_default;

-- COMMAND ----------

CREATE SCHEMA custom_schema
MANAGED LOCATION 'dbfs:/user/custom_schema.db';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse')

-- COMMAND ----------

CREATE SCHEMA custom_schema;

-- COMMAND ----------

DESCRIBE EXTENDED custom_schema;

-- COMMAND ----------

USE CATALOG hive_metastore;

CREATE SCHEMA custom_external_schema
LOCATION 'dbfs:/user/hive/warehouse';

-- COMMAND ----------

USE SCHEMA custom_external_schema;

CREATE TABLE external_table_creation_test
  (length INT, widht INT, height INT)

-- COMMAND ----------

DESCRIBE EXTENDED external_table_creation_test

-- COMMAND ----------


