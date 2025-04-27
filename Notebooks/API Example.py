# Databricks notebook source
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("price", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("image", StringType(), True),
    StructField("rating", StructType([
        StructField("rate", StringType(), True),
        StructField("count", StringType(), True)
    ]), True)
])

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ProductMaster
# MAGIC (
# MAGIC   id INT,
# MAGIC   title STRING,
# MAGIC   price DOUBLE,
# MAGIC   description STRING,
# MAGIC   category STRING,
# MAGIC   image STRING,
# MAGIC   rate DOUBLE, 
# MAGIC   count INT
# MAGIC )
# MAGIC using delta
# MAGIC location "/FileStore/tables/delta/ProductMaster"

# COMMAND ----------

# DBTITLE 1,API Call
# Replace with your actual API endpoint and authentication details
url = "https://fakestoreapi.com/products"
response = requests.get(url)
data = json.loads(response.text)
#print(data)

# COMMAND ----------

# DBTITLE 1,Spark DataFrame and Transformation

spark = SparkSession.builder.appName("ProductDataIngestion").getOrCreate()
df = spark.createDataFrame(data, schema=schema)

display(df)

# COMMAND ----------

product_df = df.select("id","title","price","description","category","image","rating.rate","rating.count")
display(product_df)

# COMMAND ----------

# DBTITLE 1,Save Data in Delta Table
product_df.write.insertInto("ProductMaster")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `ProductMaster` order by `id`