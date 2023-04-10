
# City of Cape Town - Data Science Unit Code Challenge


## Data Engineering Submission - Alec Stansell

This repository houses a solution to the Data Science unit challenge. Its broken up into three notebooks responding to items 1, 2 and 5 of the Data Science challenge.

## Development Environment

This project was developed in a Databricks environment with a spark 3.2.1 cluster with DBR runtime 10.4. Should you wish to run PySpark locally [SETUP.py](https://github.com/alecstansell/ds_code_challenge/blob/main/ACS_Submission/SETUP.md) gives detailed instructions for dependencies on a local setup. Alternatively you can use a Databricks environments with spark runtime > 10. Details of dependencies are listed per notebook.

## Notebooks

The project is broken up into three notebooks addressing each component of the challenge.

`001_Ingest_City-Hex-Polygons.py` - Ingests data via S3 and stores in apache parquet.
`002_Service_Request_H3_Join.py` - Joins H3 Polygons for the bounds of Cape Town to service requests.
`003_Bellville_South_Service_Data_Wind` - Filters for a subsample of service requests close to Bellville and (TO DO) augments with wind data and anonymises requests.


