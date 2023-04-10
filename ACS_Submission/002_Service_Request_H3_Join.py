# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC 
# MAGIC This notebook spatially joins service requests for the past 12 months with H3 indexed bounds of the City of Cape Town.
# MAGIC 
# MAGIC ## Input data
# MAGIC 
# MAGIC `bronze__city_hex_polygons` - output from previous notebook ingesting polygons from bounds of city of Cape Town
# MAGIC `sr.csv.gz` - Service requests stored in S3.
# MAGIC 
# MAGIC ## Logic
# MAGIC 
# MAGIC * Read in service request data from S3.
# MAGIC * Convert both polygon and service request data to geopandas.
# MAGIC * Spatially join.
# MAGIC * Replace the H3 index of non joined records with 0.
# MAGIC * Calculate error rate of join.
# MAGIC * Fail notebook if error exceeds threshold.
# MAGIC 
# MAGIC ## Output
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Context

# COMMAND ----------

# MAGIC %md ### Library install

# COMMAND ----------

# MAGIC %pip install geopandas python-dotenv

# COMMAND ----------

# MAGIC %md ### Import

# COMMAND ----------

from dotenv import load_dotenv
import os
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd


# COMMAND ----------

# MAGIC %md #### Secrets

# COMMAND ----------

load_dotenv()
aws_access_key = os.environ["AWS_ACCESS_KEY"]
aws_secret_key = os.environ["AWS_SECRET_KEY"]
s3_input_bucket = os.environ["S3_BUCKET"]
s3_region = os.environ["S3_REGION"] 


# COMMAND ----------

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, count

# Set up the Spark session to access the S3 bucket
spark = SparkSession.builder \
    .appName("City Hex Polygons") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3-{s3_region}.amazonaws.com") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md #### Read in Service requests
# MAGIC 
# MAGIC Read directly into memory (not S3 select).

# COMMAND ----------

s3_input_key = "sr.csv.gz"
sr_df = spark.read.format("csv").option("header", "true").load(f"s3a://{s3_input_bucket}/{s3_input_key}")

# COMMAND ----------

# MAGIC %md #### Load the City Hex Polygons from previous notebook

# COMMAND ----------

hex_df = spark.table("default.bronze__city_hex_polygons")

# COMMAND ----------

# MAGIC %md #### Pandas 
# MAGIC 
# MAGIC For scaling in future this could be refactored to geospark sedona code - just requiring additional cluster config. <br/>

# COMMAND ----------

service_requests_pd = sr_df.toPandas()
hex_polygons_pd = hex_df.toPandas()

# COMMAND ----------

# MAGIC %md #### Convert hex_polygons_pd to a GeoDataFrame

# COMMAND ----------

hex_polygons_pd['geometry'] = hex_polygons_pd['coordinates'].apply(lambda coords: Polygon(coords[0]))
hex_polygons_pd = hex_polygons_pd.rename(columns={'index': 'h3_index'})
hex_polygons_gdf = gpd.GeoDataFrame(hex_polygons_pd, geometry='geometry')

# COMMAND ----------

hex_polygons_gdf.head()

# COMMAND ----------

# MAGIC %md #### Convert service_requests_pd to a GeoDataFrame

# COMMAND ----------

service_requests_pd['geometry'] = gpd.points_from_xy(service_requests_pd['longitude'], service_requests_pd['latitude'])
service_requests_gdf = gpd.GeoDataFrame(service_requests_pd, geometry='geometry')

# COMMAND ----------

service_requests_pd.head()

# COMMAND ----------

# MAGIC %md #### Spatial Join

# COMMAND ----------

start_time = time.time()
joined_gdf = gpd.sjoin(service_requests_gdf, hex_polygons_gdf, op='within', how='left')

# COMMAND ----------

joined_gdf.head()

# COMMAND ----------

# MAGIC %md ### For any requests where the Latitude and Longitude fields are empty, set the index value to 0.

# COMMAND ----------

joined_gdf['h3_index'] = joined_gdf.apply(lambda row: 0 if pd.isna(row['latitude']) or pd.isna(row['longitude']) else row['h3_index'], axis=1)


# COMMAND ----------

# MAGIC %md ### Calculate failed join error rate

# COMMAND ----------

failed_joins = joined_gdf[joined_gdf['h3_index'] == 0]
failed_count = len(failed_joins)
total_count = len(joined_gdf)
error_rate = failed_count/total_count
print(f"Number failed joins is:\t{failed_count} ")
print(f"Error rate of join is:\t{error_rate} ")

# COMMAND ----------

# MAGIC %md #### Fail based on join error threshold
# MAGIC 
# MAGIC Chose an error of 10 % for the join for the following reasons:
# MAGIC 
# MAGIC * Accuracy of actual request data may be subject to some measurement error / logger or recording fault leading to mismatched records.
# MAGIC * H3 polygons depending on resolution can result in error where records fall between between them. 
# MAGIC * There may be overlap or mismatch across multiple areas.
# MAGIC 
# MAGIC An improvement would be to attempt a nearest neighbor join on service records that mismatch. 

# COMMAND ----------

error_threshold = 0.1
if error_rate > error_threshold:
    
    raise ValueError(f"Error rate ({error_rate}) exceeded the threshold ({error_threshold}).")

# Log time
elapsed_time = time.time() - start_time
print(f"Time taken for the operations: {elapsed_time} seconds")

