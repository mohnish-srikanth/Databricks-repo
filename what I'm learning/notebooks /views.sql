-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones (id, name, brand, year) VALUES
(1, 'iPhone 12', 'Apple', 2020),
(2, 'Galaxy S21', 'Samsung', 2021),
(3, 'Pixel 5', 'Google', 2020),
(4, 'OnePlus 9', 'OnePlus', 2021),
(5, 'Mi 11', 'Xiaomi', 2021),
(6, 'Galaxy Note 20', 'Samsung', 2020),
(7, 'iPhone 11', 'Apple', 2019);



-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE VIEW view_apple_phones  -- QUERY PERSISTED IN DATABASE
AS  SELECT *
    FROM smartphones
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT *
FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands  -- SPARK SESSION BASED
AS  SELECT DISTINCT brand
    FROM smartphones;

-- COMMAND ----------

SELECT *
FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones   -- CLUSTER SESSION BASED
AS  SELECT *
    FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

SELECT *
FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;
