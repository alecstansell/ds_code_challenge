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
# MAGIC Data is located in S3 under:
# MAGIC 
# MAGIC * Bucket: `cct-ds-code-challenge-input-data`
# MAGIC * File: `city-hex-polygons-8-10.geojson`
# MAGIC 
# MAGIC File city-hex-polygons-8.geojson contains the H3 spatial indexing system polygons and index values for the bounds of the City of Cape Town, at resolution level 8
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Logic
# MAGIC 
# MAGIC * Read data from an S3 bucket using specified access key, secret key, bucket name, and file name.
# MAGIC * Apply transformations using Spark's DataFrame API
# MAGIC   * Filter for json rows by excluding top meta data information re CRS.
# MAGIC   * Select specific columns as per schema.
# MAGIC * Assumes data is in a CSV file with one row per line and JSON data is stored within the value of the _c0 column
# MAGIC * Specifies the JSON schema using PySpark's StructType and StructField classes
# MAGIC * Requires a running Spark cluster with the necessary credentials and permissions to access the specified S3 bucket and file
# MAGIC * The resulting DataFrame can be further processed or used in downstream analysis or machine learning workflows
# MAGIC 
# MAGIC ## Output
# MAGIC 
# MAGIC 
# MAGIC ## Context

# COMMAND ----------

# MAGIC %md ## Access the file through S3 select

# COMMAND ----------


from pyspark.sql import functions as F 
from pyspark.sql import types as T 
import boto3
from pyspark.sql import SparkSession


# COMMAND ----------

# MAGIC %md #### Download file

# COMMAND ----------

# MAGIC %md #### Start spark session 
# MAGIC (Not required in databricks but if deployed / run elsewhere)

# COMMAND ----------

spark = SparkSession.builder.appName("City_Hex_Ingest").getOrCreate()

# COMMAND ----------

# MAGIC %md #### Credentials and bucket name
# MAGIC 
# MAGIC Note these could be moved to secrets within databricks or alternatively looked up from a keyvault or equivalent. <br/>
# MAGIC As the data is publicly accessible for now keeping the credentials in code is alright but better practice is moving them out and referencing them. 

# COMMAND ----------


aws_access_key = "AKIAYH57YDEWMHW2ESH2"
aws_secret_key = "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"
s3_input_bucket = "cct-ds-code-challenge-input-data"
s3_input_key = "city-hex-polygons-8-10.geojson"

# COMMAND ----------

# MAGIC %md #### S3 Select expression

# COMMAND ----------

s3_select_expression = "SELECT * FROM S3Object[*] s"

# COMMAND ----------

# MAGIC %md #### Define schema
# MAGIC 
# MAGIC Schema of the file follows:
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
# MAGIC #### Read in 
# MAGIC 
# MAGIC * Use spark read csv to read in the city-hex-polygons-8-10.geojson
# MAGIC * Filter out the first few rows

# COMMAND ----------


raw_rdd = spark.read.format("csv") \
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
    .selectExpr("s.properties.index", "s.properties.centroid_lat", "s.properties.centroid_lon", "s.geometry")

display(df)

# COMMAND ----------

# MAGIC %md #### Get meta data
# MAGIC 
# MAGIC Extract CRS info removed with above filter

# COMMAND ----------

display(raw_df.filter(F.col('_c0').like("%crs%")))
