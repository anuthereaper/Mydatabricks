# Databricks notebook source. Requires com.microsoft.azure:spark-mssql-connector_2.12:1.2.0
# Fetch raw data
import datetime
import time
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, LongType

print("Starting preparation: ", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "green"
blob_sas_token = r""
# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + wasbs_path)
# SPARK read parquet, note that it won't load any data yet by now
# NOTE - if you want to experiment with larger dataset sizes - consider switching to Option B (commenting code 
# for Option A/uncommenting code for option B) the lines below or increase the value passed into the 
# limit function restricting the dataset size below
 
#------------------------------------------------------------------------------------
# Option A - with limited dataset size
#------------------------------------------------------------------------------------
df_rawInputWithoutLimit = spark.read.parquet(wasbs_path)
partitionCount = df_rawInputWithoutLimit.rdd.getNumPartitions()
df_rawInput  = df_rawInputWithoutLimit.limit(500_000).repartition(partitionCount)
#------------------------------------------------------------------------------------
# Option B - entire dataset
#------------------------------------------------------------------------------------
#df_rawInput = spark.read.parquet(wasbs_path)

df = df_rawInput
df.persist()
df.show(5)

#[vendorID: int, lpepPickupDatetime: timestamp, lpepDropoffDatetime: timestamp, passengerCount: int, tripDistance: double, puLocationId: string, doLocationId: string, pickupLongitude: double, pickupLatitude: double, dropoffLongitude: double, dropoffLatitude: double, rateCodeID: int, storeAndFwdFlag: string, paymentType: int, fareAmount: double, extra: double, mtaTax: double, improvementSurcharge: string, tipAmount: double, tollsAmount: double, ehailFee: double, totalAmount: double, tripType: int, puYear: int, puMonth: int]>


# COMMAND ----------

#Write from Spark to SQL table using the Apache Spark Connector for SQL Server and Azure SQL
print("Use Apache Spark Connector for SQL Server and Azure SQL to write to master SQL instance ")
servername = "jdbc:sqlserver://<server_name>.database.windows.net:1433"
dbname = "<db_name>"
url = servername + ";" + "databaseName=" + dbname + ";"
dbtable = "<table_name>"
user = "<user_id>"
password = "<password>" # Please specify password here
#com.microsoft.sqlserver.jdbc.spark
try:
     df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .save()
except ValueError as error :
    print("Connector write failed", error)
print("Connector write(overwrite) succeeded  ")

# COMMAND ----------

#Use mode as append
try:
  df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("append") \
    .option("url", url) \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .save()
except ValueError as error :
    print("Connector write failed", error)
print("Connector write(append) succeeded  ")

# COMMAND ----------

#Read from SQL table using the Apache Spark Connector for SQL Server and Azure SQL
print("read data from SQL server table  ")
jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password).load()
jdbcDF.show(5)
