# Databricks notebook source
# DBTITLE 1,Clean all param
from  pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import DateType


# COMMAND ----------

# DBTITLE 1,Generating Params
#dbutils.widgets.removeAll()
dbutils.widgets.text("filepath","")
dbutils.widgets.text("filedate","","yyyymmdd")
dbutils.widgets.dropdown("historyload","false",["true","false"])

# COMMAND ----------

# DBTITLE 1,Get Param Value
filepath_str = dbutils.widgets.get("filepath")
filedate_list = dbutils.widgets.get("filedate").split(",")
historyload_flag = dbutils.widgets.get("historyload")

# COMMAND ----------

# DBTITLE 1,Assigning Local Variables
input_type="csv"
input_header = "true"
input_delimiter = ","

output_type="parquet"
output_path="/FileStore/tables/totalsales"

# COMMAND ----------

# DBTITLE 1,Function Check File Exists
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      print("***Error {} not found***".format(path))
      return False
    else:
      raise


# COMMAND ----------

def read_file_to_df(path):
    try:
        df = spark.read.format(input_type) \
            .option("inferSchema", "true") \
            .option("header", input_header) \
            .option("sep", input_delimiter) \
            .load(path) \
            .withColumn("filename", input_file_name()) #Add file name to df
        print("File path ={}; Record count ={}".format(path,df.count()))
        return df
    except Exception as e:
        print("***Error while reading file from {} path. Error:{}***".format(path,str(e)))
        return None

# COMMAND ----------

# DBTITLE 1,Creating List of Files to Read
input_file_list = []
if historyload_flag=="true":
    input_file_list.append("/" + filepath_str)
else:
    for dt in filedate_list:
        input_file_list.append("/" + filepath_str + "/DaliySales_{}.csv".format(dt))

print(input_file_list)

# COMMAND ----------

# DBTITLE 1,Read File(s)
df_sales = None
for fl in input_file_list:
    if file_exists(fl):
        df_local = read_file_to_df(fl)
        if(df_local !=None):
            df_sales = df_local if df_sales == None else df_sales.union(df_local)
if df_sales==None:
    dbutils.notebook.exit("***Error: Input file(s) not found***")
else:
    print("Number of file read ={}; Total record count ={}".format(len(input_file_list),df_sales.count()))


# COMMAND ----------

# DBTITLE 1,Persist Dataframe
#Persisting is a way of caching the intermediate results in specified storage levels so that any operations on persisted results improve performance in terms of memory usage and time
df_sales.persist()

# COMMAND ----------

display(df_sales)

# COMMAND ----------

# DBTITLE 1,Example -- 1
str1="dbfs:/FileStore/tables/salesdata/DaliySales_scsc_scsc_20250102.csv"
str2 = str1.replace(".csv","").split("_")[-1]
print(str2)

dt_str = to_timestamp(str2,'yyyymmdd')
print(dt_str)

# COMMAND ----------

# Setting an user define function: This function converts the string cell into a date:
func_str_to_date =  udf (lambda x: datetime.strptime(x.replace(".csv","").split("_")[-1], '%Y%m%d'), DateType())

# COMMAND ----------

df_sales2 = df_sales.withColumn('OrderDate', func_str_to_date(col('filename'))).drop("filename")
display(df_sales2)

# COMMAND ----------

df_sum1 = df_sales2.groupBy("CustomerID","OrderDate").sum("LineTotal")
display(df_sum1)

# COMMAND ----------

df_sum2 = df_sales2.groupBy("CustomerID","OrderDate").agg(format_number(sum("LineTotal"),2).alias("TotalSales"))
display(df_sum2)

# COMMAND ----------

# DBTITLE 1,Write Final Result into File
df_sum2.write.format(output_type).mode("overwrite").save(output_path)

# COMMAND ----------

df_sales.unpersist()

# COMMAND ----------

# DBTITLE 1,Verify Output
display(spark.read.format(output_type).load(output_path))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/totalsales