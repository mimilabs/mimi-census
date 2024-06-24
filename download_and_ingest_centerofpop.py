# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC See https://www.census.gov/geographies/reference-files/time-series/geo/centers-population.html

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------


import requests

# COMMAND ----------

path = "/Volumes/mimi_ws_1/census/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "census"
tablename = "centerofpop"

# COMMAND ----------

download_lst = [
    "https://www2.census.gov/geo/docs/reference/cenpop2020/blkgrp/CenPop2020_Mean_BG.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/tract/CenPop2020_Mean_TR.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/CenPop2020_Mean_ST.txt",
    "https://www2.census.gov/geo/docs/reference/cenpop2020/CenPop2020_Mean_US.txt"
]

volumepath = f"/Volumes/mimi_ws_1/{schema}/src/{tablename}"

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

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.census.centerofpop_bg;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.census.centerofpop_co;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.census.centerofpop_st;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.census.centerofpop_tr;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.census.centerofpop_us;

# COMMAND ----------

tablename = "centerofpop"
for filepath in Path(f"{path}/{tablename}").glob("*.txt"):
    dt = parse(filepath.stem[6:10] + '-12-31').date()
    geolevel = filepath.stem[-2:].lower()
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
    if geolevel == "bg":
        pdf["fips"] = pdf["statefp"] + pdf["countyfp"] + pdf["tractce"] + pdf["blkgrpce"]
    elif geolevel == "tr":
        pdf["fips"] = pdf["statefp"] + pdf["countyfp"] + pdf["tractce"]
    elif geolevel == "co":
        pdf["fips"] = pdf["statefp"] + pdf["countyfp"]
    pdf["mimi_src_file_date"] = dt
    pdf["mimi_src_file_name"] = filepath.name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"mimi_ws_1.census.{tablename}_{geolevel}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.census.centerofpop_bg IS '# The Center of Population for each Census Block Group
# MAGIC
# MAGIC The Center of Population for Census Block Groups is a table containing the latitude and longitude coordinates for the "center of population" for each census block group."
# MAGIC
# MAGIC The table also contains the FIPS breakdowns for state, county, and census tract. 
# MAGIC ';
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN countyfp COMMENT 'COUNTYFP:  3-character county FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN tractce COMMENT 'TRACTCE:  6-character census tract code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN blkgrpce COMMENT 'BLKGRPCE:  1-character census block group code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN fips COMMENT 'Full FIPS Code (state + county + tract + block group): 12-digits';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN mimi_src_file_date COMMENT 'The date mark of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN mimi_src_file_name COMMENT 'The name of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_bg ALTER COLUMN mimi_dlt_load_date COMMENT 'The table load date.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.census.centerofpop_co IS '# The Center of Population for each County
# MAGIC
# MAGIC The Center of Population for each County is a table containing the latitude and longitude coordinates for the "center of population" for each county."
# MAGIC
# MAGIC The table also contains the FIPS breakdowns for state, county; also contains the names of the state and county. 
# MAGIC
# MAGIC This table is useful for visualizing county-level statistics on a map.
# MAGIC ';
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN countyfp COMMENT 'COUNTYFP:  3-character county FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN fips COMMENT 'Full FIPS Code (state + county): 5-digits';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN couname COMMENT 'COUNAME:  county name';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN stname COMMENT 'STNAME:  state name';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN mimi_src_file_date COMMENT 'The date mark of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN mimi_src_file_name COMMENT 'The name of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_co ALTER COLUMN mimi_dlt_load_date COMMENT 'The table load date.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.census.centerofpop_st IS '# The Center of Population for each State
# MAGIC
# MAGIC The Center of Population for each State is a table containing the latitude and longitude coordinates for the "center of population" for each state."
# MAGIC
# MAGIC The table contains the name of the state as well.
# MAGIC
# MAGIC This table is useful for visualizing state-level statistics on a map.
# MAGIC ';
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN stname COMMENT 'STNAME:  state name';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN mimi_src_file_date COMMENT 'The date mark of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN mimi_src_file_name COMMENT 'The name of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_st ALTER COLUMN mimi_dlt_load_date COMMENT 'The table load date.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.census.centerofpop_tr IS '# The Center of Population for each Census Tract
# MAGIC
# MAGIC The Center of Population for each Census Tract is a table containing the latitude and longitude coordinates for the "center of population" for each census tract."
# MAGIC
# MAGIC The Center of Population for each Census Tract a table containing the latitude and longitude coordinates for the "center of population" for each census tract."
# MAGIC
# MAGIC The table also contains the FIPS breakdowns for state, and county. 
# MAGIC ';
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN statefp COMMENT 'STATEFP: 2-character state FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN countyfp COMMENT 'COUNTYFP:  3-character county FIPS code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN tractce COMMENT 'TRACTCE:  6-character census tract code';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN fips COMMENT 'Full FIPS Code (state + county + tract): 11-digits';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN mimi_src_file_date COMMENT 'The date mark of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN mimi_src_file_name COMMENT 'The name of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_tr ALTER COLUMN mimi_dlt_load_date COMMENT 'The table load date.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.census.centerofpop_us IS '# The Center of Population of the US
# MAGIC
# MAGIC The Center of Population for the US is a table containing the latitude and longitude coordinates for the "center of population" of the US."
# MAGIC
# MAGIC The information is useful for centering a map visualization. 
# MAGIC ';
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN population COMMENT 'POPULATION:  2020 Census population tabulated for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN latitude COMMENT 'LATITUDE:  latitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN longitude COMMENT 'LONGITUDE:  longitude coordinate for the center of population for the block group';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN mimi_src_file_date COMMENT 'The date mark of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN mimi_src_file_name COMMENT 'The name of the source file.';
# MAGIC ALTER TABLE mimi_ws_1.census.centerofpop_us ALTER COLUMN mimi_dlt_load_date COMMENT 'The table load date.';

# COMMAND ----------


