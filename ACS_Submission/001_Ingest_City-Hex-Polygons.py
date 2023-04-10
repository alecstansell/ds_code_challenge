# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC 
# MAGIC This notebook ingests city-hex-polygons-8-10.geojson via S3 Select. 
# MAGIC 
# MAGIC 
# MAGIC ## Input data
# MAGIC 
# MAGIC Input is located in S3 under:
# MAGIC 
# MAGIC * Bucket: `cct-ds-code-challenge-input-data`
# MAGIC * File: `city-hex-polygons-8-10.geojson`
# MAGIC 
# MAGIC 
# MAGIC `city-hex-polygons-8-10.geojson`: contains the H3 spatial indexing system polygons and index values for resolution levels 8, 9 and 10, for the City of Cape Town.
# MAGIC 
# MAGIC `city-hex-polygons-8.geojson`: contains the H3 spatial indexing system polygons and index values for the bounds of the City of Cape Town, at resolution level 8. Its used for validation.
# MAGIC 
# MAGIC 
# MAGIC ## Logic
# MAGIC 
# MAGIC * Read data from an S3 bucket using specified access key, secret key, bucket name, and file name.
# MAGIC * Apply transformations using Spark's DataFrame API
# MAGIC   * Filter for json rows by excluding top meta data information re CRS.
# MAGIC   * Select specific columns as per schema.
# MAGIC * Specify the JSON schema using PySpark's StructType and StructField classes
# MAGIC * The resulting DataFrame can be further processed or used in downstream analysis or machine learning workflows
# MAGIC 
# MAGIC ## Output
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Context

# COMMAND ----------

# MAGIC %pip install apache-sedona

# COMMAND ----------

# MAGIC %md ### Libraries

# COMMAND ----------

# MAGIC %pip install python-dotenv 

# COMMAND ----------


from pyspark.sql import functions as F 
from pyspark.sql import types as T 
import boto3
from pyspark.sql import SparkSession
import time
import logging
import os
from dotenv import load_dotenv


# COMMAND ----------

# MAGIC %md ### Start spark session 
# MAGIC (Not required in databricks but if deployed / run elsewhere)

# COMMAND ----------

spark = SparkSession.builder.appName("City_Hex_Ingest").getOrCreate()

# COMMAND ----------

# MAGIC %md ### Credentials and S3 details
# MAGIC 
# MAGIC * Look up credentials in env file. If these were sensitive credentials they could be moved to AWS secrets or a keyvault.

# COMMAND ----------

load_dotenv()
aws_access_key = os.environ["AWS_ACCESS_KEY"]
aws_secret_key = os.environ["AWS_SECRET_KEY"]
s3_input_bucket = os.environ["S3_BUCKET"]
s3_region = os.environ["S3_REGION"] 
s3_input_key = "city-hex-polygons-8-10.geojson"

# COMMAND ----------

# MAGIC %md ### S3 Select expression
# MAGIC 
# MAGIC Can modify accordingly for larger files

# COMMAND ----------

s3_select_expression = "SELECT * FROM S3Object[*] s"

# COMMAND ----------

# MAGIC %md ### Define schema
# MAGIC 
# MAGIC Schema of the file:
# MAGIC 
# MAGIC * `type`: string field that specifies the type of the object.
# MAGIC * `properties`: json object containing:
# MAGIC   * `index`: H3 index
# MAGIC   * `centroid_lat`: a double field that specifies the latitude of the centroid of the H3 hexagon.
# MAGIC   * `centroid_lon`: a double field that specifies the longitude of the centroid of the H3 hexagon.
# MAGIC   * `resolution`: an integer field that specifies the resolution of the H3 index.
# MAGIC * `geometry`: a struct field that contains information about the geometry of the hexagon. It has two sub-fields: 
# MAGIC   * `type`: a string field that specifies the type of the geometry. "Polygon" for this schema.
# MAGIC   * `coordinates`: an array field that contains the coordinates of the hexagon. It is a nested array of double values, with each nested array representing a set of coordinates for a single polygon.

# COMMAND ----------


schema = T.StructType([
    T.StructField("type", T.StringType()),
    T.StructField("properties", T.StructType([
        T.StructField("index", T.StringType()),
        T.StructField("centroid_lat", T.DoubleType()),
        T.StructField("centroid_lon", T.DoubleType()),
        T.StructField("resolution", T.IntegerType())
    ])),
    T.StructField("geometry", T.StructType([
        T.StructField("type", T.StringType()),
        T.StructField("coordinates", T.ArrayType(T.ArrayType(T.ArrayType(T.DoubleType()))))
    ]))
])


# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Read in 
# MAGIC 
# MAGIC * Use spark read csv to read in the city-hex-polygons-8-10.geojson
# MAGIC * Filter for valid json rows.

# COMMAND ----------

# MAGIC %md #### Add logging

# COMMAND ----------


# Create a logger
logger = logging.getLogger(__name__)

# Create a FileHandler to write log messages to a file
handler = logging.FileHandler('/dbfs/test/log/ingest_log.log')

# Set the logging level and format
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

# COMMAND ----------

# MAGIC %md #### Read in data

# COMMAND ----------

logging.debug("Starting data ingestion...")
start_time = time.time()

raw_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .option("delimiter", "\n") \
    .option("maxCharsPerRecord", 1048576) \
    .option("quote", "'") \
    .option("escape", "'") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .load("s3a://%s/%s" % (s3_input_bucket, s3_input_key)) 
 
df = raw_df.filter("_c0 LIKE '{ %'") \
    .select(F.from_json("_c0", schema).alias("s")) \
    .selectExpr("s.properties.index", "s.properties.centroid_lat", "s.properties.centroid_lon", "s.geometry.type", "s.geometry.coordinates")

# Collect to see speed of full extraction
# df = df.collect()

logging.info("Data extraction completed in %s seconds." % (time.time() - start_time))

# COMMAND ----------

# MAGIC %md ### View meta data
# MAGIC 
# MAGIC Extract CRS info removed with above filter

# COMMAND ----------

display(raw_df.filter(F.col('_c0').like("%crs%")))

# COMMAND ----------

# MAGIC %md ### Validate
# MAGIC 
# MAGIC To validate against the data download the entire df `city-hex-polygons-8.geojson` (as opposed to querying it)
# MAGIC 
# MAGIC Compare to the s3 ingested file.

# COMMAND ----------

dbutils.fs.mkdirs("test/fixture")
dbutils.fs.mkdirs("test/log")

# COMMAND ----------

s3_file_path = 'city-hex-polygons-8.geojson'
s3 = boto3.client('s3', region_name=s3_region, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
local_file_path = '/dbfs/test/fixture/city-hex-polygons-8.geojson'
s3.download_file(s3_input_bucket, s3_file_path, local_file_path)

# COMMAND ----------

val_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .option("delimiter", "\n") \
    .option("maxCharsPerRecord", 1048576) \
    .option("quote", "'") \
    .option("escape", "'") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv("/fixture/city-hex-polygons-8.geojson") \
    .filter("_c0 LIKE '{ %'") \
    .select(F.from_json("_c0", schema).alias("s")) \
    .selectExpr("s.properties.index", "s.properties.centroid_lat", "s.properties.centroid_lon", "s.geometry.type", "s.geometry.coordinates")


# COMMAND ----------

# MAGIC %md ### Check for different index records
# MAGIC 
# MAGIC This has some limitations:
# MAGIC * The route to ingestion is still the same - ie spark csv / we could use a different route such as with geospark sedonna.
# MAGIC * This test only covers if the indexed records are the same

# COMMAND ----------

result = val_df.join(df, val_df.index == df.index, 'anti')
assert result.count() == 0, "Validation failed: unexpected records found in the joined DataFrame"

# COMMAND ----------

# MAGIC %md ### Save to Databricks
# MAGIC Save to parquet delta table format in databricks for use downstream.

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.bronze__city_hex_polygons")

# COMMAND ----------

# MAGIC %md ### View data

# COMMAND ----------

# MAGIC %sql select * from default.bronze__city_hex_polygons limit 10 
