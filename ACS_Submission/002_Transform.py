# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC 
# MAGIC 
# MAGIC ## Input data
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Logic
# MAGIC 
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

hex_polygons_gdf

# COMMAND ----------

# MAGIC %md #### Convert service_requests_pd to a GeoDataFrame

# COMMAND ----------

service_requests_pd['geometry'] = gpd.points_from_xy(service_requests_pd['longitude'], service_requests_pd['latitude'])
service_requests_gdf = gpd.GeoDataFrame(service_requests_pd, geometry='geometry')

# COMMAND ----------

service_requests_pd

# COMMAND ----------

# MAGIC %md #### Spatial Join

# COMMAND ----------

start_time = time.time()
joined_gdf = gpd.sjoin(service_requests_gdf, hex_polygons_gdf, op='within', how='left')

# COMMAND ----------

# MAGIC %md ### For any requests where the Latitude and Longitude fields are empty, set the index value to 0.

# COMMAND ----------

joined_gdf['h3_index'] = joined_gdf.apply(lambda row: 0 if pd.isna(row['latitude']) or pd.isna(row['longitude']) else row['h3_index'], axis=1)


# COMMAND ----------

failed_joins = joined_gdf[joined_gdf['h3_index'] == 0]
failed_count = len(h3_index_zero)
total_count = len(joined_gdf)
error_rate = failed_count/total_count
print(f"Number of failed joins is:\t{failed_count} ")

# COMMAND ----------

error_threshold = 0.1
if error_rate > error_threshold:
    raise ValueError(f"Error rate ({error_rate}) exceeded the threshold ({error_threshold}).")

# Calculate and log the time taken for the operations
elapsed_time = time.time() - start_time
print(f"Time taken for the operations: {elapsed_time} seconds")

