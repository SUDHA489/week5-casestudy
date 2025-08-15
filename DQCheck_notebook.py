# Databricks notebook source
# MAGIC %pip install pydeequ
# MAGIC %restart_python

# COMMAND ----------

import os
os.environ["SPARK_VERSION"] = "3.5"

# COMMAND ----------

print("Reading raw IoT data")
df = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/raw_data")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite,VerificationResult
import json

check = Check(spark, CheckLevel.Error, "IoT Sensor Data Quality")\
    .isContainedIn("Status", ["OK", "WARNING", "ALERT"])

result = VerificationSuite(spark).onData(df).addCheck(check).run()


if result.status != "Success":
    print("Deequ checks failed.")
else:
    print("Deequ checks passed")

print("Deequ results:-")
result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
display(result_df)

output_path = f"dbfs:/FileStore/shared_uploads/traininguser8@sudosu.ai/deequ_reports"
result_df.write.mode("overwrite").json(output_path)
results_json = [json.loads(row.value) for row in spark.read.text(output_path).collect()]
print(json.dumps(results_json, indent=2))

print("DQ Check complete.")
