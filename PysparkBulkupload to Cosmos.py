# Databricks notebook source
cosmosEndpoint = "https://xxxxxxxxxx.documents.azure.com:443/"
cosmosMasterKey = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
cosmosDatabaseName = "xxxxxx"
cosmosContainerName = "xxxxxxx"

# COMMAND ----------

import uuid
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.views.repositoryPath", "/viewDefinitions" + str(uuid.uuid4()))#

# COMMAND ----------

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
df_rawInput = df_rawInputWithoutLimit.limit(50_000).repartition(partitionCount)
df_rawInput.persist()
 
#------------------------------------------------------------------------------------
# Option B - entire dataset
#------------------------------------------------------------------------------------
#df_rawInput = spark.read.parquet(wasbs_path)
 
# Adding an id column with unique values
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
nowUdf= udf(lambda : int(time.time() * 1000),LongType())
df_input_withId = df_rawInput \
  .withColumn("id", uuidUdf()) \
  .withColumn("insertedAt", nowUdf()) \
 
print('Register the DataFrame as a SQL temporary view: source')
df_input_withId.createOrReplaceTempView('source')
print("Finished preparation: ", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
print(spark.sql('SELECT * FROM source'))

# COMMAND ----------

import uuid
import datetime
 
print("Starting ingestion: ", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
 
writeCfg = {
  "spark.cosmos.accountEndpoint": cosmosEndpoint,
  "spark.cosmos.accountKey": cosmosMasterKey,
  "spark.cosmos.database": cosmosDatabaseName,
  "spark.cosmos.container": cosmosContainerName,
  "spark.cosmos.write.bulk.enabled": "true",
  "spark.cosmos.write.point.maxConcurrency": "5",
  "spark.cosmos.write.bulk.maxPendingOperations": "1000"
}
 
print("Selecting from dataframe: ", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
df_NYCGreenTaxi_Input = spark.sql('SELECT * FROM source')
 
print("Starting ingestion: ", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
df_NYCGreenTaxi_Input \
  .write \
  .format("cosmos.oltp") \
  .mode("Append") \
  .options(**writeCfg) \
  .save()
 
print("Finished ingestion: ", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

# COMMAND ----------

count_source = spark.sql('SELECT * FROM source').count()
print("Number of records in source: ", count_source)
