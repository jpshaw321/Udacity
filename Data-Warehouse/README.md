# Project Data Warehouse:

## Project description:
Sparkify is a music streaming startup that is looking to optimize its data management and analytics capabilities. Their data is currently stored in Amazon S3 as JSON logs on user activity and metadata of songs. 
An ETL pipeline is needed to extract data from S3, stage it in Redshift and carry out transformations to create the fact table and dimension tables.

## How to run:
Open a terminal in the project folder and enter the following commands:
python create_tables.py
etl.py

## Project files:
create_table.py is where the  fact and dimension tables for the star schema in Redshift are created.
etl.py is where the data from S3 is loaded into the staging tables on Redshift and then processed into the analytics tables on Redshift.
sql_queries.py is where the SQL statements are defined, which will be imported into the two other files above.

## Database schema design:
staging_events - This is an exact copy of the original json data.
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
userId INTEGER

staging_songs - Again an exact copy of the original json data.
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER

songplays - records in event data associated with song plays i.e. records with page NextSong.
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP SORTKEY DISTKEY,
user_id INTEGER NOT NULL,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR

users - users in the app.
user_id INTEGER PRIMARY KEY SORTKEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR

songs - songs in the music database.
song_id VARCHAR PRIMARY KEY SORTKEY,
title VARCHAR,
artist_id VARCHAR,
year INTEGER,
duration FLOAT

artists - artists in the music database.
artist_id VARCHAR PRIMARY KEY SORTKEY,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT

time - timestamps of records in songplays broken down into specific units.
start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday VARCHAR

## ETL pipeline:
1. Extract data from the S3 bucket.
2. Load data to the staging tables.
3. Transform the data to create the fact table and dimension tables.




