# Mydatabricks
AES_encryption_py.py : AES encryption

AvrotoJson.scala : Convert AVRO to JSON format using SCALA and Python

Mount ADLS.py : Mount a ADLS to databricks using SPN

PGP_encryption_py.py : PGP encryption using python

PysparkBulkupload to Cosmos.py : Bulk upload to Cosmos using Pyspark. Needs the com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.4.0 Maven library.

DF_toMSSQL.py : Generate random data and load dataframe to SQL using Pyspark
CREATE TABLE emptable(
[id] [INT]  NULL,
[emp_id] [INT] NULL,
[firstname] [NVARCHAR] (MAX) NULL,
[lastname] [NVARCHAR] (MAX) NULL,
[salary] [DECIMAL] NULL,
[start_date] [NVARCHAR] (MAX) NULL,
[email] [NVARCHAR] (MAX) NULL,
[phone] [DECIMAL] NULL
)

PySpark_toMSSQL1.py : Load random data into SQL using Pyspark
CREATE TABLE Mytable(
[vendorID] [INT]  NULL,
[lpepPickupDatetime] [DATE] NULL,
[lpepDropoffDatetime] [DATE] NULL,
[passengerCount] [INT] NULL,
[tripDistance] [DECIMAL] NULL,
[puLocationId] [NVARCHAR](MAX) NULL,
[doLocationId] [NVARCHAR](MAX) NULL,
[pickupLongitude] [NVARCHAR](MAX) NULL,
[pickupLatitude] [NVARCHAR](MAX) NULL,
[dropoffLongitude] [NVARCHAR](MAX) NULL,
[dropoffLatitude] [NVARCHAR](MAX) NULL,
[rateCodeID] [INT] NULL,
[storeAndFwdFlag] [NVARCHAR](MAX) NULL,
[paymentType] [INT] NULL,
[fareAmount] [DECIMAL]  NULL,
[extra] [DECIMAL] NULL,
[mtaTax] [DECIMAL] NULL,
[improvementSurcharge] [NVARCHAR](MAX) NULL,
[tipAmount] [DECIMAL] NULL,
[tollsAmount] [DECIMAL] NULL,
[ehailFee] [DECIMAL] NULL,
[totalAmount] [DECIMAL]NULL,
[tripType] [INT] NULL,
[puYear] [INT] NULL,
[puMonth] [INT] NULL
)


RSA_encryption_py.py : RSA encryption using python

SparkEventhub_Read_Python.py : Read Eventhub using Pyspark

SparkEventhub_Read_Scala.scala : Read Eventhub using SCALA

Unzip_aes_py.py : Unzip using aes in Python
