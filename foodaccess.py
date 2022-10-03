# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/Data_Food_Access_2019.csv"
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

# COMMAND ----------

df.write.format("delta").saveAsTable("foodaccess.data2019")

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Variable_Lookup_Food_Access_2019.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfdatadict = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(dfdatadict)
dfdatadict.printSchema()

# COMMAND ----------

dfdatadict.write.format("delta").saveAsTable("foodaccess.datadictionary")
