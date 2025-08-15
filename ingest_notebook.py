# Databricks notebook source
dbutils.widgets.text("source_path","dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/iot_data.csv")
source = dbutils.widgets.get("source_path")
print(source)

# COMMAND ----------

df = spark.read.csv(source, header=True, inferSchema=True)
df.write.mode("overwrite").format("delta").save("dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/raw_data")

print("Ingestion complete and saved data")