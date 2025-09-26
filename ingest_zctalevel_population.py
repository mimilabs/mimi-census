# Databricks notebook source
# the dataset downloaded from https://data.census.gov/table/DECENNIALDHC2020.P1?q=All%205-digit%20ZIP%20Code%20Tabulation%20Areas%20within%20United%20States%20Populations%20and%20People

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.census.pop_est_zcta

# COMMAND ----------

import pandas as pd
import csv
data = []
path = '/Volumes/mimi_ws_1/census/src/dhc/'
filename = 'DECENNIALDHC2020.P1-Data.csv'
with open(f'{path}{filename}', 'r') as fp:
    reader = csv.reader(fp)
    next(reader)
    next(reader)
    for row in reader:
        name = row[1].split()[1]
        num = int(row[2])
        data.append([row[0], name, num, 2020])
pdf = pd.DataFrame(data, columns=["geo_id", "zcta", "tot_population_est", "year"])
pdf['mimi_src_file_date'] = parse('2020-12-31').date()
pdf['mimi_src_file_name'] = filename
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write
    .format("delta")
    .mode("overwrite")
    .option('mergeSchema', 'true')
    .saveAsTable("mimi_ws_1.census.pop_est_zcta"))    


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN geo_id COMMENT 'Geographic ID by the US Census';
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN zcta COMMENT 'ZIP Code Tabulation Area (ZCTA); note that ZCTA is slightly different from ZIP, but mostly the same. For more information, please see [https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html)';
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN tot_population_est COMMENT 'Population estimate';
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN year COMMENT 'The year of the estimate; currenly 2020.';
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN mimi_src_file_date COMMENT 'The date mark of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN mimi_src_file_name COMMENT 'The name of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.pop_est_zcta ALTER COLUMN mimi_dlt_load_date COMMENT 'The table load date.';

# COMMAND ----------


