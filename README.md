**Introduction**

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We need to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their 
analytics team to continue finding insights in what songs their users are listening to. 
You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with 
their expected results.

**Project Description**

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. 
To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics 
tables from these staging tables.

**Project Datasets**
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data

Log data: s3://udacity-dend/log_data

Log data json path: s3://udacity-dend/log_json_path.json

Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json

song_data/A/A/B/TRAABJL12903CDCF1A.json
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1,
 "artist_id": "ARJIE2Y1187B994AB7",
 "artist_latitude": null,
 "artist_longitude": null,
 "artist_location": "",
 "artist_name": "Line Renaud",
 "song_id": "SOUPIRU12A6D4FA1E1",
 "title": "Der Kleine Dompfaff",
 "duration": 152.92036,
 "year": 0}
Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json

log_data/2018/11/2018-11-13-events.json


**Repo Contents**

README.md : This file, explaining the purpose and usage of the Repo

LICENCE.txt : Details of the licence for this project

create_tables.py : Creates the required tables in a redshift staging area

etl.py : The pipeline itself; this loads the datafiles into the database tables

sql_queries.py : A list of the SQL queries to be used to create tables and load data

Built With
Python
AWS - Redshift
Getting Started
To get a local copy up and running follow these simple steps.

Prerequisites
A Redshift cluster on AWS
A Python IDE
A dwh.cfg file must be present in the same location as the project files, using the following format:
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
The empty fields must be completed with the relevant information from the Redshift clsuter and associated IAM role.

Usage

Launch a redshift cluster and create an IAM role that has read access to S3.

Add redshift database and IAM role info to dwh.cfg.

Run 'create_tables.py' to create the postgres create_tables

Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

Implement the logic in etl.py to load data from S3 to staging tables on Redshift and to load data from those staging tables to analytics tables on Redshift.

Test by running etl.py after running create_tables.py and running the analytic queries (sample query provided below) on your Redshift database to compare your results with the expected results.

Delete your redshift cluster when finished.

Sample query

To test that the tables have loaded correctly - the following query calculates the most 5 frequently played song, calling on the songplays, artists and songs tables:

SELECT a.name, so.title, COUNT(so.title) FROM songplays sp
JOIN artists a
ON a.artist_id = sp.artist_id
JOIN songs so
ON sp.song_id = so.song_id
GROUP BY a.name, so.title
ORDER BY COUNT(so.title) DESC
LIMIT 5

This should return the following:

Dwight Yoakam	You're The One	37
Ron Carter	I CAN'T GET STARTED	9
Lonnie Gordon	Catch You Baby (Steve Pitron & Max Sanna Radio Edit)	9
B.o.B	Nothin' On You [feat. Bruno Mars] (Album Version)	8
Usher featuring Jermaine Dupri	Hey Daddy (Daddy's Home)	6

Schema Design and ETL Pipeline

The initial step in the pipeline is to copy the existing tables from s3 into the staging tables in the Redshift cluster. The staging tables have the 
same field names as the source tables, and do not have any restrictions - i.e:

No PRIMARY KEY

No NOT NULLs

Only varchar, integer and bigint data types.

The fact table 'songplays' is then defined as follows:

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays"
                         "("
                         "songplay_id integer identity(1,1) PRIMARY KEY, "
                         "start_time timestamp, "
                         "user_id integer, "
                         "level varchar, "
                         "song_id varchar, "
                         "artist_id varchar, "
                         "session_id integer, "
                         "location varchar, "
                         "user_agent varchar"
                         ")"
                         "SORTKEY(start_time);"
                         )
This creates a new serial column as the PRIMARY KEY (called IDENTITTY in Redshift)

The dimension tables can be seen in the 'sql_queries.py' script, and have been designed as follows:

User table - user_id is set as PRIMARY KEY and gender is set as char(1) as this only contains 'M' or 'F'

Songs table - song_id is set as PRIMARY KEY

Artists table - artist_id is set as PRIMARY KEY

Time table - start_time is set as a PRIMARY KEY. The timestamp is then broken out into hour, day, week, month, year and weekday.

In all tables FOREIGN KEYs have a NOT NULL constraint

Once these tables have been created they are then populated using a series of INSERT statements:

The songplays table is defined by forming an INNER JOIN on both staging tables, joining on song title, artist name and length of song. This ensures that each is a definite match, and results in 319 songs being populated into the table.

The user table is populated from the events staging table, with a NOT_NULL constraint on the PRIMARY KEY

The song table is populated from the songs staging table, with a NOT_NULL constraint on the PRIMARY KEY

The artist table is populated from the songs staging table.

The time table is populated from the events table, using EXTRACT (from start_time) commands to break down the start time into parts which an analyst may find more usable.

In each case SELECT DISTINCT statements are used to filter duplicate entries.
