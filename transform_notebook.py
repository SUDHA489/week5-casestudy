# Databricks notebook source
print("Reading raw iot data")
df = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/raw_data")

from pyspark.sql.functions import *

df_transformed = df.withColumn("Temperature_F", round(col("Temperature_C") * 9/5 + 32, 2))

df_transformed.show(5)

df_transformed.write.mode("overwrite").format("delta").save("dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/transformed_data")

print("Transformation complete and saved data")
