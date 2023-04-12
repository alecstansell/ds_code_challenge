
# City of Cape Town - Data Science Unit Code Challenge


## Cape Town Challenge Submission: Data Engineering Components by Alec Stansell

This repository contains a solution for the data engineering items of the City of Cape Town Data Science Unit Challenge. The solution is divided into three notebooks, each addressing a specific item from the challenge.

## Development Environment

This project was developed in a Databricks environment with a spark 3.2.1 cluster with DBR runtime 10.4. For local setup [SETUP.py](https://github.com/alecstansell/ds_code_challenge/blob/main/ACS_Submission/SETUP.md) file provides detailed instructions on the local requirements for running PySpark, which is used extensively in the project.

Alternatively, the code can be run or deployed on any Spark cluster or Databricks environment with Spark runtime version 10 or higher. Dependency details are provided for each notebook.

## Notebooks

The project consists of three notebooks addressing each component of the challenge:

* **[001_Ingest_City-Hex-Polygons.py](https://github.com/alecstansell/ds_code_challenge/blob/main/ACS_Submission/001_Ingest_City-Hex-Polygons.py)**: Ingests data via S3 select and stores in Apache Parquet delta table format.
* **[002_Service_Request_H3_Join.py](https://github.com/alecstansell/ds_code_challenge/blob/main/ACS_Submission/002_Service_Request_H3_Join.py)**: Joins H3 Polygons for the bounds of Cape Town to service requests.
* **[003_Bellville_South_Service_Data_Wind](https://github.com/alecstansell/ds_code_challenge/blob/main/ACS_Submission/003_Bellville_South_Service_Data_Wind.py)**: Filters for a subsample of service requests close to Bellville and  augments with wind data / anonymises requests *(TO DO)*.

The SETUP.md file is included to guide the local PySpark development environment setup.
Alternatively, access to the Databricks development environment in which the code was developed can be provided upon request.

## Note to reviewers

* Unfortunately due to time constraints over the Easter weekend I've yet to complete the wind augmentation and anonymisation components of task 3. Both are relatively simple - augmentation I'll just pull from the API and spatial join as needed, anonymise I'll likely hash any sensitive information / create keys as necessary. 

Thanks for the fun challenge! I'm keen to solve the other elements - Data Science and visualisation. I'll continue comitting to this repo over the next day or so shout if you have any questions or are interested in a conversation with me on my thinking for the challenge.


