# Databricks notebook source
# the dataset downloaded from https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/historical-delineation-files.html

# https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

today = datetime.today().date()

# COMMAND ----------

path = '/Volumes/mimi_ws_1/census/src/cbsa/'
for filepath in Path(path).glob('list1*'):
    pdf = pd.read_excel(filepath, skiprows=2, dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf['mimi_src_file_date'] = parse(filepath.stem[-4:]+"-12-31").date()
    pdf['mimi_src_file_name'] = filepath.name
    pdf['mimi_dlt_load_date'] = today
    df = spark.createDataFrame(pdf)
    (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"mimi_src_file_name = '{filepath.name}'")
        .saveAsTable('mimi_ws_1.census.cbsa_to_metro_div_csa'))

# COMMAND ----------

path = '/Volumes/mimi_ws_1/census/src/cbsa/'
for filepath in Path(path).glob('list2*'):
    pdf = pd.read_excel(filepath, skiprows=2, dtype=str)
    pdf.columns = change_header(pdf.columns)
    
    pdf['mimi_src_file_date'] = parse(filepath.stem[-4:]+"-12-31").date()
    pdf['mimi_src_file_name'] = filepath.name
    pdf['mimi_dlt_load_date'] = today
    df = spark.createDataFrame(pdf)
    (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"mimi_src_file_name = '{filepath.name}'")
        .saveAsTable('mimi_ws_1.census.cbsa_to_principal_city_fips'))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.census.cbsa_to_metro_div_csa IS '# [Core based statistical areas (CBSAs), metropolitan divisions, and combined statistical areas (CSAs)](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html) | resolution: cbsa, interval: yearly';
# MAGIC COMMENT ON TABLE mimi_ws_1.census.cbsa_to_principal_city_fips IS '# [Principal cities of metropolitan and micropolitan statistical areas](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html) | resolution: cbsa, interval: yearly';
# MAGIC */

# COMMAND ----------


