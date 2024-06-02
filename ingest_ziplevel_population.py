# Databricks notebook source
# the dataset downloaded from https://data.census.gov/table/DECENNIALDHC2020.P1?q=All%205-digit%20ZIP%20Code%20Tabulation%20Areas%20within%20United%20States%20Populations%20and%20People

# COMMAND ----------

import pandas as pd
import csv
data = []
with open('/Volumes/mimi_ws_1/census/src/dhc/DECENNIALDHC2020.P1-Data.csv', 'r') as fp:
    reader = csv.reader(fp)
    next(reader)
    next(reader)
    for row in reader:
        name = row[1].split()[1]
        num = int(row[2])
        data.append([row[0], name, num, 2020])
pdf = pd.DataFrame(data, columns=["geo_id", "ztca", "tot_population_est", "year"])
df = spark.createDataFrame(pdf)
(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.census.pop_est_zcta"))    


# COMMAND ----------


