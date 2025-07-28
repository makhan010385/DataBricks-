# Databricks notebook source
# DBTITLE 1,Import Required Modules
#Step 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, sum


# COMMAND ----------

# DBTITLE 1,# Create a SparkSession
spark = SparkSession.builder.appName("AmazonSalesDataAnalysis").getOrCreate()

# COMMAND ----------

# DBTITLE 1,# Load the dataset into a DataFrame
df = spark.read.csv("dbfs:/FileStore/AmazonSalesData.csv", header=True, inferSchema=True)

# COMMAND ----------

# DBTITLE 1,# Group the data by region and calculate the total profit for each region
region_and_country_with_profit = df.groupBy(col('Region')).sum('Total Profit')

# COMMAND ----------

# DBTITLE 1,Show the Region and Country With Profit
region_and_country_with_profit.show() 

# COMMAND ----------

# DBTITLE 1,# Order the regions by total profit in descending order and get the region with the highest profit
