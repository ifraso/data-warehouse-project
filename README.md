# Data Warehouse Project

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpore of this project is to build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to.

## Description

The project consists in the following files:

- *dwh.cfg* is a configuration file that includes a set of parametrs needed to access the cluster

- *sql_queries.py* is a script where all the queries are defined to be used in other scripts

- *create_tables.py* is a script that first connects to the DWH, then drops the tables and ricreates them

- *etl.py* is a script that first connects to the DWH, then executes the copy (from s3 to staging tables) and insert statements (from staging to target tables)

- *iac.ipynb* is a Jupyter notebook for creating a Redshift cluster 

- *README.md* is a configuration file that includes a set of parametrs needed to access the cluster

## How to

To execute the project the following scripts must be executed in order:

1. *create_tables.py*

2. *etl.py*

The last script execution may take a while depending on cluster configuration due to the size of imput files.