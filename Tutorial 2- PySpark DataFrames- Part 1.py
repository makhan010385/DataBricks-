# Databricks notebook source
# MAGIC %md
# MAGIC #### In this file We will Cover
# MAGIC - PySpark Dataframe 
# MAGIC - Reading The Dataset
# MAGIC - Checking the Datatypes of the Column(Schema)
# MAGIC - Selecting Columns And Indexing
# MAGIC - Check Describe option similar to Pandas
# MAGIC - Adding Columns
# MAGIC - Dropping columns
# MAGIC - Renaming Columns

# COMMAND ----------

# MAGIC %md
# MAGIC The SparkContext is used to access the underlying Spark environment and perform operations on it. The SparkSession, on the other hand, is used to access the data stored in Spark and perform operations on it.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.appName('Dataframe').getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

## read the dataset
df_pyspark=spark.read.option('header','true').csv('/FileStore/tables/test1.csv',inferSchema=True)

# COMMAND ----------

### Check the schema
df_pyspark.printSchema()

# COMMAND ----------

df_pyspark=spark.read.csv('test1.csv',header=True,inferSchema=True)
df_pyspark.show()

# COMMAND ----------

### Check the schema
df_pyspark.printSchema()

# COMMAND ----------

type(df_pyspark)

# COMMAND ----------

df_pyspark.head(3)

# COMMAND ----------

df_pyspark.show()

# COMMAND ----------

df_pyspark1=df_pyspark.select(['Name','Experience']).show()

# COMMAND ----------

df_pyspark1.show()

# COMMAND ----------

df_pyspark['Name']

# COMMAND ----------

df_pyspark.dtypes

# COMMAND ----------

df_spark2=df_pyspark.describe().show()

# COMMAND ----------

df_spark2.show()

# COMMAND ----------

### Adding Columns in data frame
df_pyspark=df_pyspark.withColumn('Experience After 2 year',df_pyspark['Experience']+2)

# COMMAND ----------

df_pyspark.show()

# COMMAND ----------

### Drop the columns
df_pyspark=df_pyspark.drop('Experience After 2 year')

# COMMAND ----------

df_pyspark.show()

# COMMAND ----------

### Rename the columns
df_pyspark.withColumnRenamed('Name','New Name').show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

