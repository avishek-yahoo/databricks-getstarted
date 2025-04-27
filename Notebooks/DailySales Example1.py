# Databricks notebook source
# DBTITLE 1,Generating Params
#dbutils.widgets.removeAll()
dbutils.widgets.text("filepath","")
dbutils.widgets.text("filedate","","yyyymmdd")
dbutils.widgets.dropdown("historyload","false",["true","false"])

# COMMAND ----------

# DBTITLE 1,Get Param Value
filepath_str = dbutils.widgets.get("filepath")
print(filepath_str)
filedate_list = dbutils.widgets.get("filedate").split(",")
print(filedate_list)
historyload_flag = dbutils.widgets.get("historyload")
print(historyload_flag)

# COMMAND ----------

# DBTITLE 1,Assigning Local Variables
input_type="csv"
input_header = "true"
input_delimiter = ","

output_type="parquet"

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
            .load(path)
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

df_sales.unpersist()