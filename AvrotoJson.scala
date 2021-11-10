// Databricks notebook source
# Your ADLS should have already been mounted.
import org.apache.spark.sql
 
 val df = spark.read.format("avro")
    .load("/mnt/anuadlstest/33.avro")
  df.show()

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
// convert body from binary to string
val messages =
  df
  .withColumn("Body", $"Body".cast(StringType))
  .select("Body")

// COMMAND ----------

messages.createOrReplaceTempView("json_table")   //write spark data frame to python dataframe

// COMMAND ----------

// MAGIC %python
// MAGIC import json
// MAGIC 
// MAGIC json_file = '/dbfs/mnt/anuadlstest/33.json'
// MAGIC 
// MAGIC python_table = sql("select * from json_table")
// MAGIC pandasdf = python_table.toPandas()
// MAGIC result = []
// MAGIC # loop through all rows in dataframe
// MAGIC for index, row in pandasdf.iterrows():   
// MAGIC     res = json.loads(row['Body'])   # convert string to dictionary
// MAGIC     result.append(res)
// MAGIC json_file = json.dumps(result, indent=4)    # convert dictionary to JSON
// MAGIC print(json_file)
// MAGIC with open(json_file, "w") as text_file:
// MAGIC     text_file.write(json_file)

// COMMAND ----------


