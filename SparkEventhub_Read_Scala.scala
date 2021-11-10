// Databricks notebook source
import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
// Build connection string with the above information
val namespaceName = "xxxxx"
val eventHubName = "xxxxxx"
val sasKeyName = "xxxxxxx"
val sasKey = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)
 
val customEventhubParameters =
  EventHubsConf(connStr.toString()).setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(5)
 
//val customEventhubParameters =
//  EventHubsConf(connStr.toString())
//  .setMaxEventsPerTrigger(5)

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
 
val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
 
incomingStream.printSchema
 
// Sending the incoming stream into the console.
// Data comes in batches!
//incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
 
// Event Hub message format is JSON and contains "body" field
// Body is binary, so we cast it to string to see the actual content of the message
val messages =
  incomingStream
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  .withColumn("Partition", $"partition".cast(StringType))
  .withColumn("PartitionKey", $"partitionKey".cast(StringType))
  .select("Offset", "Time (readable)", "Timestamp", "Body","Partition","PartitionKey")
 
messages.printSchema
 
messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
