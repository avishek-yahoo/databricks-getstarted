# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/masterdata/CustomerMaster.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table
temp_table_name = "CustomerMaster"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `CustomerMaster`

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct AddressType,AddressLine1,AddressLine2,City,StateProvince,CountryRegion,PostalCode from CustomerMaster where PostalCode is not null

# COMMAND ----------

customer_df = df.select("FullName","EmailAddress","Phone").distinct()
display(customer_df)

# COMMAND ----------

file_location = "/FileStore/tables/masterdata/CustomerData"
customer_df.write.format("parquet").mode("overwrite").save(file_location)

# COMMAND ----------

for f in dbutils.fs.ls("/FileStore/tables/masterdata/CustomerData"):
    if("parquet" in f.name):
        print("file name=>{} of size=>{}".format(f.name,f.size))


# COMMAND ----------

permanent_table_name = "CustomerData"
dbutils.fs.rm("dbfs:/user/hive/warehouse/customerdata",True)
customer_df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CustomerData limit 100

# COMMAND ----------

address_df=spark.sql("select distinct EmailAddress,AddressType,AddressLine1,AddressLine2,City,StateProvince,CountryRegion,PostalCode from CustomerMaster where PostalCode is not null")


# COMMAND ----------

file_location = "/FileStore/tables/masterdata/CustomerAddress"
address_df.write.format("parquet").mode("overwrite").save(file_location)

# COMMAND ----------

for f in dbutils.fs.ls("/FileStore/tables/masterdata/CustomerAddress"):
    if("parquet" in f.name):
        print("file name=>{} of size=>{}".format(f.name,f.size))