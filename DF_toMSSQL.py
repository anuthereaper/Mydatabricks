# Databricks notebook source
pip install names

# COMMAND ----------

# Generate random data and load to MSSQL using pyspark
import pandas as pd
import json
from pandas import json_normalize
from random import randrange
import random
import names
from datetime import datetime
from datetime import timedelta
import uuid

no_of_rows = 50000
random.seed(1999)
d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('1/1/2009 4:50 AM', '%m/%d/%Y %I:%M %p')

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def generate_random_data(i):
    emp_id = random.randint(1000, 5000)
    firstname = names.get_first_name()
    lastname = names.get_last_name()
    salary = random.randint(12000, 2000000)
    start_date = str(random_date(d1,d2))
    uuidx = str(uuid.uuid4())
    email = firstname + lastname + "@xyz.com"
    phone = random.randint(9900000000, 9999999999)
    return ({"id":i, "emp_id" : emp_id, "firstname":firstname, "lastname": lastname, "salary" : salary, "start_date" : start_date, "email" : email, "phone": phone})
    
starttime = datetime.utcnow()
print("Start of random data creation: ", starttime.strftime("%Y-%m-%d %H:%M:%S.%f"))
# create an Empty DataFrame object
df = pd.DataFrame()
for i in range(no_of_rows):
    msg = generate_random_data(i)
    df2 = json_normalize(msg)
    df = df.append(df2, ignore_index = True)
    if (i % 10000 == 0):
        print(str(i) + " documents generated")
    
#print(df)
endtime = datetime.utcnow()
print("End of process: ", endtime.strftime("%Y-%m-%d %H:%M:%S.%f"))
print("Time taken for " + str(no_of_rows) + " rows: " + str(endtime-starttime))


# COMMAND ----------

from pyspark.sql import SparkSession
#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
#Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(df) 
sparkDF.printSchema()
sparkDF.show(5)

# COMMAND ----------

#Write from Spark to SQL table using the Apache Spark Connector for SQL Server and Azure SQL
print("Use Apache Spark Connector for SQL Server and Azure SQL to write to master SQL instance ")
servername = "jdbc:sqlserver://<server_name>.database.windows.net:1433"
dbname = ""<dbname>"
url = servername + ";" + "databaseName=" + dbname + ";"
dbtable = "<table>"
user = "<userid>"
password = "<password>" # Please specify password here
#com.microsoft.sqlserver.jdbc.spark
try:
     sparkDF.write \
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
