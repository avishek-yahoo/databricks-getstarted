# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/student_master.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.createOrReplaceTempView("student_master")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `student_master`