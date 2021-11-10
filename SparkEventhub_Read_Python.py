# Databricks notebook source
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from pyspark.sql.functions import split, explode,lit 
from pyspark.sql.functions import from_json, col,udf
from pyspark.sql.types import StructType, StructField, StringType,ArrayType
from datetime import datetime
import time
import datetime
from pyspark.sql.types import *
from  pyspark.sql.functions import regexp_replace,col
import re
 
connectionString = "Endpoint=sb://xxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxx;EntityPath=xxxxxxxxxxxxx"
 
ehConf = {}
ehConf['eventhubs.connectionString'] =  sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf['eventhubs.consumerGroup'] = "$Default"
ehConf['maxEventsPerTrigger']=15000
 
df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
 
df1 = df.withColumn("payload",F.col("body").cast(StringType())).select("payload")
 
df1.select("*") \
         .writeStream \
         .format("csv") \
         .option("header", "true")\
         .option("path", "/mnt/anuadlstest/") \
         .option("checkpointLocation", "dbfs:/mnt/anuadlstest/test1/") \
         .trigger(processingTime='20 seconds') \
         .start() 
df1.select("*") \
         .writeStream \
         .format("console") \
         .trigger(processingTime='20 seconds') \
         .start() 
