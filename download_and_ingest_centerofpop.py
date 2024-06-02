# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC See https://www.census.gov/geographies/reference-files/time-series/geo/centers-population.html

# COMMAND ----------


import requests

# COMMAND ----------

download_lst = [
    "https://www2.census.gov/geo/docs/reference/cenpop2020/blkgrp/CenPop2020_Mean_BG.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/tract/CenPop2020_Mean_TR.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/CenPop2020_Mean_ST.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/CenPop2020_Mean_US.txt"
]

volumepath = "/Volumes/mimi_ws_1/census/src/centerofpop"

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

for url in download_lst:
    filename = url.split("/")[-1]
    download_file(url, filename, volumepath)

# COMMAND ----------

from pathlib import Path
import pandas as pd
from dateutil.parser import parse
path = "/Volumes/mimi_ws_1/census/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "census"
tablename = "centerofpop"

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.census.centerofpop_bg;
# MAGIC --DROP TABLE mimi_ws_1.census.centerofpop_co;
# MAGIC --DROP TABLE mimi_ws_1.census.centerofpop_st;
# MAGIC --DROP TABLE mimi_ws_1.census.centerofpop_tr;
# MAGIC --DROP TABLE mimi_ws_1.census.centerofpop_us;

# COMMAND ----------

tablename = "centerofpop"
for filepath in Path(f"{path}/{tablename}").glob("*.txt"):
    dt = parse(filepath.stem[6:10] + '-12-31').date()
    geolevel = filepath.stem[-2:]
    pdf = pd.read_csv(filepath, dtype={
            "STATEFP":  str,
            "COUNTYFP":  str,
            "TRACTCE":  str,
            "BLKGRPCE":  str,
            "POPULATION":  int,
            "LATITUDE":  float,
            "LONGITUDE":  float
        })
    pdf.columns = [x.lower() for x in pdf.columns]
    pdf["_input_file_date"] = dt
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"mimi_ws_1.census.{tablename}_{geolevel}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN countyfp COMMENT 'COUNTYFP:  3-character county FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN tractce COMMENT 'TRACTCE:  6-character census tract code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN blkgrpce COMMENT 'BLKGRPCE:  1-character census block group code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN countyfp COMMENT 'COUNTYFP:  3-character county FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN couname COMMENT 'COUNAME:  county name';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN stname COMMENT 'STNAME:  state name';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN stname COMMENT 'STNAME:  state name';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN countyfp COMMENT 'COUNTYFP:  3-character county FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN tractce COMMENT 'TRACTCE:  6-character census tract code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';

# COMMAND ----------


