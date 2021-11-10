# Databricks notebook source
pip install pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from datetime import datetime
from pyspark.sql.window import Window
 
connectionString = "Endpoint=sb://xxxxxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=master;SharedAccessKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;EntityPath=xxxxxxxxxxxxx"
ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString
ehConf['eventhubs.consumerGroup'] = "$Default"
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
 
process_time = '1 minute'

# COMMAND ----------

# checkpoint paths having indexes
CheckpointPath="dbfs:/mnt/anuadlstest/test2/chkpoint/"
 
Optin_CheckpointPath="dbfs:/mnt/anuadlstest/test2/optinchk/"
Update_CheckpointPath="dbfs:/mnt/anuadlstest/test2/updchk/"
Transaction_CheckpointPath="dbfs:/mnt/anuadlstest/test2/txnchk/"
Removal_CheckpointPath="dbfs:/mnt/anuadlstest/test2/rmvchk/"
 
# Temp paths having part files
Optin_TempPath="dbfs:/mnt/anuadlstest/test2/optin/"
Update_TempPath="dbfs:/mnt/anuadlstest/test2/update/"
Transaction_TempPath="dbfs:/mnt/anuadlstest/test2/txn/"
Removal_TempPath="dbfs:/mnt/anuadlstest/test2/removal/"
 
# Temp paths having part files
Optin_TempPath2='dbfs:/mnt/anuadlstest/test2/optin_'    
Update_TempPath2='dbfs:/mnt/anuadlstest/test2/update_'
Transaction_TempPath2='dbfs:/mnt/anuadlstest/test2/txn_'
 
# Destination paths having final renamed files with suffix as revision date
Optin_DestinationPath="dbfs:/mnt/anuadlstest/test2/optindest/"
Update_DestinationPath="dbfs:/mnt/anuadlstest/test2/upddest/"
Transaction_DestinationPath="dbfs:/mnt/anuadlstest/test2/txndest/"
Removal_DestinationPath="dbfs:/mnt/anuadlstest/test2/rmvdest/"

# COMMAND ----------

# Define the schemas for all the objects
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType
 
events_schema_Optin = StructType([
  StructField("UUID", StringType(), True),
  StructField("BonusLinkPAN", StringType(), True),
  StructField("VehicleNumPlate", StringType(), True),
  StructField("RFIDtag", StringType(), True),
  StructField("FullName", StringType(), True),
  StructField("MobileNum", StringType(), True),
  StructField("Email", StringType(), True),
  StructField("ServiceIndicator", StringType(), True),
  StructField("ServiceIndicatorDateTime", StringType(), True),
  StructField("ServiceName", StringType(), True),
  StructField("MrktCommIndicator", StringType(), True),
  StructField("MrktCommIndicatorDateTime", StringType(), True),
  StructField("SourceSystem", StringType(), True),
  StructField("MessageID", StringType(), True),
  StructField("IPaasTimeStamp", StringType(), True)
])
 
events_schema_Update = StructType([
  StructField("UUID", StringType(), True),
  StructField("BonusLinkPAN", StringType(), True),
  StructField("VehicleNumPlate", StringType(), True),
  StructField("temperature", StringType(), True),
  StructField("RFIDtag", StringType(), True),
  StructField("FullName", StringType(), True),
  StructField("MobileNum", StringType(), True),
  StructField("Email", StringType(), True),
  StructField("ServiceIndicator", StringType(), True),
  StructField("ServiceIndicatorDateTime", StringType(), True),
  StructField("ServiceName", StringType(), True),
  StructField("MrktCommIndicator", StringType(), True),
  StructField("MrktCommIndicatorDateTime", StringType(), True),
  StructField("SourceSystem", StringType(), True),
  StructField("UpdateTimeStamp", StringType(), True),
  StructField("MessageID", StringType(), True),
  StructField("IPaasTimeStamp", StringType(), True)
])
 
events_schema_Removal=StructType([
  StructField("UUID", StringType(), True),
  StructField("RemovalIndicator", StringType(), True),
  StructField("RemovalIndicatorDateTime", StringType(), True),
  StructField("IPaasTimeStamp", StringType(), True),
  StructField("MessageID", StringType(), True)
  ])

events_schema_Transaction = StructType([
                 StructField("UUID", StringType(), True),
                 StructField("VehicleNumPlate", StringType(), True),
                 StructField("RFIDtag", StringType(), True),
                 StructField("RequestData", StructType([
                 StructField("RequestID", StringType(), True),
                 StructField("PayTokenID", StringType(), True),])),
                 StructField("MobilePaymentData", StructType([
                 StructField("GlobalRetailSiteID", StringType(), True),
                 StructField("PumpNumber", IntegerType(), True),
                 StructField("MethodOfPaymentID", StringType(), True),
                 StructField("MethodOfPaymentName", StringType(), True),
                 StructField("PaymentServiceProvider", StringType(), True),
                 StructField("LoyaltyDetails", ArrayType(
                 StructType([
                 StructField("LoyaltyPAN", StringType(), True),
                 StructField("Trackers", ArrayType(
                 StructType([
                 StructField("TrackerType", StringType(), True),
                 StructField("TrackerValue", StringType(), True),
                 StructField("TrackerEarned", StringType(), True),
                 StructField("TrackerRedeemed", StringType(), True)])),True),])),True)])),
                 StructField("SaleTransaction", StructType([
                 StructField("TransactionNumber", StringType(), True),
                 StructField("TimeStamp", StringType(), True),
                 StructField("TotalAmount", StringType(), True),
                 StructField("TotalDiscountAmount", StringType(), True),
                 StructField("TotalTaxAmount", StringType(), True),
                 StructField("ExtraDiscountAmount", StringType(), True),
                 StructField("CurrencyCode", StringType(), True),
                 StructField("LoyaltyPointAmount", StringType(), True),
                 StructField("Authorization", StructType([
                 StructField("TerminalID", StringType(), True),
                 StructField("AuthTimeStamp", StringType(), True),
                 StructField("ApprovalCode", StringType(), True),
                 StructField("MerchantName", StringType(), True),
                 StructField("SiteName", StringType(), True),]))])),
                 StructField("SaleItems", ArrayType(
                 StructType([
                 StructField("LineNo", StringType(), True),
                 StructField("ProductCode", StringType(), True),
                 StructField("ProductDescription", StringType(), True),
                 StructField("AdditionalProductCode", StringType(), True),
                 StructField("UnitOfMeasure", StringType(), True),
                 StructField("UnitOfMeasure", StringType(), True),
                 StructField("Quantity", StringType(), True),
                 StructField("UnitPrice", StringType(), True), 
                 StructField("NetUnitPrice", StringType(), True),
                 StructField("Amount", StringType(), True),
                 StructField("NetAmount", StringType(), True),
                 StructField("OriginalAmount", StringType(), True),
                 StructField("OriginalNetAmount", StringType(), True),
                 StructField("DiscountAmount", StringType(), True),
                 StructField("NetAmount1", StringType(), True), 
                 StructField("TaxRate", StringType(), True),
                 StructField("TaxAmount", StringType(), True),
                 StructField("PriceAdjustments", ArrayType(
                 StructType([
                 StructField("PriceAdjustmentID", StringType(), True),
                 StructField("ProductCode", StringType(), True),
                 StructField("NetAmount", StringType(), True),
                 StructField("Amount", StringType(), True),
                 StructField("UnitPrice", StringType(), True),
                 StructField("UnitOfMeasure", StringType(), True), 
                 StructField("Quantity", StringType(), True),
                 StructField("Reason", StringType(), True),])), True),])), True) ,
                 StructField("MessageID", StringType(), True),
                 StructField("IPaasTimeStamp", StringType(), True)])

# COMMAND ----------

def ExecutionLog(LogDate, LogType, Severity, ErrorDesc, LogText, ObjectID, Irowcount, Orowcount, Rrowcount, Collected, sourcePresent, Success, Source_FileName):
    success_list = StructType ([StructField("TaskLogDate",StringType(), True),
                                  StructField("LogTypeID",StringType(), True),
                                  StructField("SeverityID", StringType(), True),
                                  StructField("ErrorDesc", StringType(), True),
                                  StructField("LogText", StringType(), True),
                                  StructField("IngestionObjectID", StringType(), True),
                                  StructField("InputRowCount", StringType(), True),
                                  StructField("OutPutRowCount", StringType(), True),
                                  StructField("RejectedRowCount", StringType(), True),
                                  StructField("IsCollected", StringType(), True),
                                  StructField("IsSourcePresent", StringType(), True),
                                  StructField("IsSuccess", StringType(), True),
                                  StructField("Source_FileName", StringType(), True)
                                 ])
 
    successList = [(LogDate, LogType, Severity, ErrorDesc, LogText, ObjectID, Irowcount, Orowcount, Rrowcount, Collected, sourcePresent, Success, Source_FileName)]
    successlistDF = spark.createDataFrame (successList,success_list)        
    successlistDF.write.mode("append").jdbc(RDPSqlDbUrlSmall, "RDIP.TaskLog")

# COMMAND ----------

df = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

from datetime import datetime
 
def process_batch(df, epochId):
    df.show()
    now = datetime.now() 
    ct = now.strftime("%Y_%m_%d_%H_%M_%S")
    #df.coalesce(1).write.format('json').save('/mnt/anuadlstest/file_' + str(ct) + '.json')
  
    decoded_df_Optin =df.select(F.from_json(F.col("body").cast("string"),events_schema_Optin).alias("Payload_Optin"), F.col("partition").alias("Partition"))
    decoded_df_Optin= decoded_df_Optin.filter(decoded_df_Optin.Partition=="0")
 
    decoded_df_Update =df.select(F.from_json(F.col("body").cast("string"),events_schema_Update).alias("Payload_Update"), F.col("partition").alias("Partition"))
    decoded_df_Update= decoded_df_Update.filter(decoded_df_Update.Partition=="1")
 
    decoded_df_Transaction =df.select(F.from_json(F.col("body").cast("string"),events_schema_Transaction).alias("Payload_Transaction"), F.col("partition").alias("Partition"))
    decoded_df_Transaction= decoded_df_Transaction.filter(decoded_df_Transaction.Partition=="2")
    df_events_Optin=decoded_df_Optin.select(decoded_df_Optin.Payload_Optin.UUID,decoded_df_Optin.Payload_Optin.BonusLinkPAN,decoded_df_Optin.Payload_Optin.VehicleNumPlate,decoded_df_Optin.Payload_Optin.RFIDtag,decoded_df_Optin.Payload_Optin.FullName,decoded_df_Optin.Payload_Optin.MobileNum,decoded_df_Optin.Payload_Optin.Email,decoded_df_Optin.Payload_Optin.ServiceIndicator,decoded_df_Optin.Payload_Optin.ServiceIndicatorDateTime,decoded_df_Optin.Payload_Optin.ServiceName,decoded_df_Optin.Payload_Optin.MrktCommIndicator,decoded_df_Optin.Payload_Optin.MrktCommIndicatorDateTime,decoded_df_Optin.Payload_Optin.SourceSystem,decoded_df_Optin.Payload_Optin.MessageID,decoded_df_Optin.Payload_Optin.IPaasTimeStamp)
    df_events_optin_column_renamed=df_events_Optin.withColumnRenamed("Payload_Optin.UUID","UUID").withColumnRenamed("Payload_Optin.BonusLinkPAN","BonusLinkPAN").withColumnRenamed("Payload_Optin.RFIDtag","RFIDtag").withColumnRenamed("Payload_Optin.FullName","FullName").withColumnRenamed("Payload_Optin.Email","Email").withColumnRenamed("Payload_Optin.ServiceIndicator","ServiceIndicator").withColumnRenamed("Payload_Optin.MrktCommIndicator","MrktCommIndicator").withColumnRenamed("Payload_Optin.MrktCommIndicatorDateTime","MrktCommIndicatorDateTime").withColumnRenamed("Payload_Optin.SourceSystem","SourceSystem").withColumnRenamed("Payload_Optin.MessageID","MessageID").withColumnRenamed("Payload_Optin.IPaasTimeStamp","IPaasTimeStamp").withColumnRenamed("Payload_Optin.VehicleNumPlate","VehicleNumPlate").withColumnRenamed("Payload_Optin.MobileNum","MobileNum").withColumnRenamed("Payload_Optin.ServiceIndicatorDateTime","ServiceIndicatorDateTime").withColumnRenamed("Payload_Optin.ServiceName","ServiceName")
    #df_events_optin_column_renamed.coalesce(1).write.format('json').save(Optin_TempPath)
    #df_events_optin_column_renamed.write.json(Optin_TempPath2 + str(ct) + '.json',mode='append')
    df_events_optin_column_renamed.write.json(Optin_TempPath,mode='append')  #  This will create separate part files in 1 single folder
    #df_events_optin_column_renamed.toPandas().to_json(Optin_TempPath2 + str(ct) + '.json', orient='records', force_ascii=False, lines=True)
 
    df_events_update=decoded_df_Update.select(decoded_df_Update.Payload_Update.UUID,decoded_df_Update.Payload_Update.BonusLinkPAN,decoded_df_Update.Payload_Update.VehicleNumPlate,decoded_df_Update.Payload_Update.RFIDtag,decoded_df_Update.Payload_Update.FullName,decoded_df_Update.Payload_Update.MobileNum,decoded_df_Update.Payload_Update.Email,decoded_df_Update.Payload_Update.ServiceIndicator,decoded_df_Update.Payload_Update.ServiceIndicatorDateTime,decoded_df_Update.Payload_Update.ServiceName,decoded_df_Update.Payload_Update.MrktCommIndicator,decoded_df_Update.Payload_Update.MrktCommIndicatorDateTime,decoded_df_Update.Payload_Update.SourceSystem,decoded_df_Update.Payload_Update.UpdateTimeStamp,decoded_df_Update.Payload_Update.MessageID,decoded_df_Update.Payload_Update.IPaasTimeStamp)
 
    df_events_update_column_renamed=df_events_update.withColumnRenamed("Payload_Update.UUID","UUID").withColumnRenamed("Payload_Update.BonusLinkPAN","BonusLinkPAN").withColumnRenamed("Payload_Update.RFIDtag","RFIDtag").withColumnRenamed("Payload_Update.FullName","FullName").withColumnRenamed("Payload_Update.Email","Email").withColumnRenamed("Payload_Update.ServiceIndicator","ServiceIndicator").withColumnRenamed("Payload_Update.MrktCommIndicator","MrktCommIndicator").withColumnRenamed("Payload_Update.MrktCommIndicatorDateTime","MrktCommIndicatorDateTime").withColumnRenamed("Payload_Update.SourceSystem","SourceSystem").withColumnRenamed("Payload_Update.MessageID","MessageID").withColumnRenamed("Payload_Update.IPaasTimeStamp","IPaasTimeStamp").withColumnRenamed("Payload_Update.VehicleNumPlate","VehicleNumPlate").withColumnRenamed("Payload_Update.ServiceIndicatorDateTime","ServiceIndicatorDateTime").withColumnRenamed("Payload_Update.ServiceName","ServiceName").withColumnRenamed("Payload_Update.UpdateTimeStamp","UpdateTimeStamp").withColumnRenamed("Payload_Update.MobileNum","MobileNum")
 
    #df_events_update_column_renamed.coalesce(1).write.format('json').save(Update_TempPath)
    #df_events_update_column_renamed.write.json(Update_TempPath2 + str(ct) + '.json',mode='append')
    df_events_update_column_renamed.write.json(Update_TempPath,mode='append')  #  This will create separate part files in 1 single folder
    #df_events_update_column_renamed.toPandas().to_json(Update_TempPath2 + str(ct) + '.json', orient='records', force_ascii=False, lines=True)
 
    Trans_Df_select=decoded_df_Transaction.select(decoded_df_Transaction.Payload_Transaction.UUID,decoded_df_Transaction.Payload_Transaction.VehicleNumPlate,decoded_df_Transaction.Payload_Transaction.RFIDtag,decoded_df_Transaction.Payload_Transaction.RequestData,decoded_df_Transaction.Payload_Transaction.MobilePaymentData,decoded_df_Transaction.Payload_Transaction.SaleTransaction,decoded_df_Transaction.Payload_Transaction.SaleItems,decoded_df_Transaction.Payload_Transaction.MessageID,decoded_df_Transaction.Payload_Transaction.IPaasTimeStamp)
 
    df_events_Transaction_column_renamed=Trans_Df_select.withColumnRenamed("Payload_Transaction.UUID","UUID").withColumnRenamed("Payload_Transaction.VehicleNumPlate","VehicleNumPlate").withColumnRenamed("Payload_Transaction.RFIDtag","RFIDtag").withColumnRenamed("Payload_Transaction.RequestData","RequestData").withColumnRenamed("Payload_Transaction.MobilePaymentData","MobilePaymentData").withColumnRenamed("Payload_Transaction.SaleTransaction","SaleTransaction").withColumnRenamed("Payload_Transaction.SaleItems","SaleItems").withColumnRenamed("Payload_Transaction.MessageID","MessageID").withColumnRenamed("Payload_Transaction.IPaasTimeStamp","IPaasTimeStamp")
 
    #df_events_Transaction_column_renamed.coalesce(1).write.format('json').save(Transaction_TempPath)
    #df_events_Transaction_column_renamed.write.json(Transaction_TempPath2 + str(ct) + '.json',mode='append')
    df_events_Transaction_column_renamed.write.json(Transaction_TempPath,mode='append')    #  This will create separate part files in 1 single folder
    #df_events_Transaction_column_renamed.toPandas().to_json(Transaction_TempPath2 + str(ct) + '.json', orient='records', force_ascii=False, lines=True)
    
query = (df.writeStream.trigger(processingTime=process_time).foreachBatch(process_batch).outputMode("update").option("checkpointLocation", CheckpointPath).start())
