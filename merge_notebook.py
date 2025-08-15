# Databricks notebook source
print("Reading transformed data...")
df_transformed = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/transformed_data")

print("Reading DQ results JSON...")
dq_json = spark.read.text("dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/deequ_reports").collect()[0][0]
print("DQ Report:", dq_json)

print("Sample transformed data:")
df_transformed.show(5)

print("Final report generated")
