# Databricks notebook source

import requests

# COMMAND ----------

download_lst = [
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2019/cps-redesign/asec19_takeup.sas7bdat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2018/cps-redesign/pubuse_esioffer_2018.sas7bdat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2017/cps-redesign/pubuse_esioffer_2017.sas7bdat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/pubuse_esioffer_2016.sas7bdat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/ppint15esi_offer_ext.sas7bdat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/ppint14esi_offer_ext.sas7bdat"
]
download_lst2 = [
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2018/cps-redesign/asec18_currcov_extract.dat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2017/cps-redesign/asec17_currcov_extract.dat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2016/cps-redesign/asec16_currcov_extract.dat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec15_currcov_extract.dat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_now_anycov_redes.dat",
    "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_now_anycov.dat"
]
volumepath = "/Volumes/mimi_ws_1/census/src/cps_asec"

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

for url in download_lst2:
    filename = url.split("/")[-1]
    download_file(url, filename, volumepath)
