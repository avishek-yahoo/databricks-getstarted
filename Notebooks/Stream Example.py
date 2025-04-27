# Databricks notebook source
from pyspark.sql.functions import rand

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/delta/Impressions",True)
spark.sql("drop table if exists Impressions")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Impressions
# MAGIC (
# MAGIC   adId long,
# MAGIC   impressionTime timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/FileStore/tables/delta/Impressions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Impressions

# COMMAND ----------

#In absence of actual data streams, we are going to generate fake data streams using our built-in "rate stream", that generates data at a given fixed rate.
impressions = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "2").option("numPartitions", "1").load()
    .selectExpr("value AS adId", "timestamp AS impressionTime")
)

# COMMAND ----------

#Let's see what data these two streaming DataFrames generate.
display(impressions)

# COMMAND ----------

#Write data into deltalake table
impressions.writeStream.trigger(availableNow=True).option("checkpointLocation", "/FileStore/tables/checkpoint/impressions").toTable("Impressions")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Impressions;
# MAGIC --select * from Impressions order by impressionTime desc limit 20