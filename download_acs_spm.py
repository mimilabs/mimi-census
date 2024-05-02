# Databricks notebook source

import requests

# COMMAND ----------

download_lst = [
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2022.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2021.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2019.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2018.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2017.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2016.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2015.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2014.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2013.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2012.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2011.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2010.sas7bdat",
    "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_pu_2009.sas7bdat"]
volumepath = "/Volumes/mimi_ws_1/census/src/acs_spm"

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


