# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

def read_file_to_df(path):
    try:
        df = spark.read.format("csv") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(path) \
            .withColumn("filename", input_file_name())
        print("File path ={}; Record count ={}".format(path,df.count()))
        return df
    except Exception as e:
        print("***Error while reading file from {} path. Error:{}***".format(path,str(e)))
        return None

# COMMAND ----------

func_str_to_date =  udf (lambda x: datetime.strptime(x.replace(".csv","").split("_")[-1], '%Y%m%d'), DateType())

# COMMAND ----------

# DBTITLE 1,Drop Table
spark.sql("drop table if exists DailySales")
dbutils.fs.rm("/FileStore/tables/delta/DailySales",True)


# COMMAND ----------

# DBTITLE 1,Create Delta Table
# MAGIC %sql
# MAGIC create table if not exists DailySales
# MAGIC (
# MAGIC   SalesOrderID integer,
# MAGIC   OrderQty integer,
# MAGIC   ProductID integer,
# MAGIC   UnitPrice double,
# MAGIC   UnitPriceDiscount double,
# MAGIC   LineTotal double,
# MAGIC   CustomerID integer,
# MAGIC   OrderDate date
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (OrderDate)
# MAGIC location "/FileStore/tables/delta/DailySales"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DailySales

# COMMAND ----------

df1=read_file_to_df("/FileStore/tables/salesdata/DaliySales_20250101.csv").withColumn('OrderDate', func_str_to_date(col('filename'))).drop("filename")
display(df1)

# COMMAND ----------

# Delta table insert using pyspark
df1.write.insertInto("DailySales")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from DailySales limit 10;
# MAGIC
# MAGIC select count(*) as Cnt,OrderDate from DailySales group by OrderDate;

# COMMAND ----------

df2=read_file_to_df("/FileStore/tables/salesdata/DaliySales_20250102.csv").withColumn('OrderDate', func_str_to_date(col('filename'))).drop("filename")
df2.createOrReplaceTempView("vw_DailySales")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use SQL to insert data into the Delta table
# MAGIC INSERT INTO DailySales SELECT * FROM vw_DailySales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from DailySales group by OrderDate;

# COMMAND ----------

df3=read_file_to_df("/FileStore/tables/salesdata").withColumn('OrderDate', func_str_to_date(col('filename'))).drop("filename")

# Creating STG delta table
df3.write.format("delta").mode("overwrite").saveAsTable("Temp_DailySales")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from Temp_DailySales group by OrderDate;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Use SQL to upsert data into the Delta table
# MAGIC MERGE INTO DailySales tgt
# MAGIC USING Temp_DailySales src
# MAGIC ON src.SalesOrderID = tgt.SalesOrderID
# MAGIC AND src.ProductID = tgt.ProductID
# MAGIC AND src.CustomerID = tgt.CustomerID
# MAGIC AND src.OrderDate = tgt.OrderDate
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.OrderQty=src.OrderQty,
# MAGIC     tgt.UnitPrice=src.UnitPrice,
# MAGIC     tgt.UnitPriceDiscount=src.UnitPriceDiscount,
# MAGIC     tgt.LineTotal=tgt.LineTotal
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     SalesOrderID,
# MAGIC     OrderQty,
# MAGIC     ProductID,
# MAGIC     UnitPrice,
# MAGIC     UnitPriceDiscount,
# MAGIC     LineTotal,
# MAGIC     CustomerID,
# MAGIC     OrderDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.SalesOrderID,
# MAGIC     src.OrderQty,
# MAGIC     src.ProductID,
# MAGIC     src.UnitPrice,
# MAGIC     src.UnitPriceDiscount,
# MAGIC     src.LineTotal,
# MAGIC     src.CustomerID,
# MAGIC     src.OrderDate
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from DailySales group by OrderDate;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DailySales

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from DailySales VERSION AS OF 2 group by OrderDate

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from DailySales TIMESTAMP AS OF '2025-04-24T04:41:00.000' group by OrderDate

# COMMAND ----------

# MAGIC %sql
# MAGIC Restore Table DailySales To VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from DailySales group by OrderDate

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE DailySales

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DailySales

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM DailySales

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DailySales

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Cnt,OrderDate from DailySales TIMESTAMP AS OF '2025-04-24T04:51:03.000' group by OrderDate