# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC 
# MAGIC This notebook finds service requests within a minute of Bellville and augments with weather data and anonymises the requests. 
# MAGIC 
# MAGIC 
# MAGIC ## Input data
# MAGIC 
# MAGIC `sr_hex.csv.gz` - Service requests stored in S3.
# MAGIC 
# MAGIC 
# MAGIC ## Logic
# MAGIC 
# MAGIC * Read in service request data from S3.
# MAGIC * Convert point  and service request data to geopandas.
# MAGIC * Find the centroid of Bellville South
# MAGIC * Find the sub sample of requests that are within a minute of Bellville South
# MAGIC * Augment with wind data
# MAGIC * Anonymise
# MAGIC 
# MAGIC ## Output
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Context

# COMMAND ----------

# MAGIC %md ### Library install

# COMMAND ----------

# MAGIC %pip install geopandas python-dotenv geopy

# COMMAND ----------

# MAGIC %md ### Import

# COMMAND ----------

from dotenv import load_dotenv
import os
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd
from geopy.geocoders import GoogleV3
from shapely.geometry import Point

# COMMAND ----------

# MAGIC %md #### Secrets

# COMMAND ----------

load_dotenv()
aws_access_key = os.environ["AWS_ACCESS_KEY"]
aws_secret_key = os.environ["AWS_SECRET_KEY"]
s3_input_bucket = os.environ["S3_BUCKET"]
s3_region = os.environ["S3_REGION"] 
maps_api_key = os.environ["MAPS_API_KEY"] 


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

s3_input_key = "sr_hex.csv.gz"
sr_df = spark.read.format("csv").option("header", "true").load(f"s3a://{s3_input_bucket}/{s3_input_key}")
service_requests_pd = sr_df.toPandas()

# COMMAND ----------

service_requests_pd['geometry'] = gpd.points_from_xy(service_requests_pd['longitude'], service_requests_pd['latitude'])
service_requests_gdf = gpd.GeoDataFrame(service_requests_pd, geometry='geometry')

# COMMAND ----------

# MAGIC %md #### Google Maps API to retrieve Bellville centroid location
# MAGIC 
# MAGIC Use the Maps API to find the centroid of Bellville South, South Africa. 
# MAGIC * Note that Maps does not return a polygon of Bellville South rather a single point that aproximates its centroid - ie the centre of BELLVILLE SOUTH.
# MAGIC * Requires creation of an API key with google maps

# COMMAND ----------

geolocator = GoogleV3(api_key=maps_api_key)
place_name = "BELLVILLE SOUTH, South Africa"
location = geolocator.geocode(place_name)
centroid = location.longitude, location.latitude
centroid_bellville_south = Point(centroid) 

# COMMAND ----------

# MAGIC %md #### Use buffer for join to get subsample

# COMMAND ----------

buffer_distance_km = 1 / 60  # 1 minute in decimal degrees (approximately)
bellville_south_buffer = centroid_bellville_south.buffer(buffer_distance_km)
intersects_gdf_buffer = service_requests_gdf[service_requests_gdf['geometry'].intersects(bellville_south_buffer)]
intersects_gdf_buffer.head()

# COMMAND ----------

# MAGIC %md #### Augment with wind data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Anonymise 
# MAGIC 
# MAGIC Likely hash the sensitive info.
