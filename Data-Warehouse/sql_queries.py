import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
  (
     artist        VARCHAR,
     auth          VARCHAR,
     firstname     VARCHAR,
     gender        CHAR(1),
     iteminsession INTEGER,
     lastname      VARCHAR,
     length        FLOAT,
     level         VARCHAR,
     location      VARCHAR,
     method        VARCHAR,
     page          VARCHAR,
     registration  FLOAT,
     sessionid     INTEGER,
     song          VARCHAR,
     status        INTEGER,
     ts            BIGINT,
     useragent     VARCHAR,
     userid        INTEGER
  ) 
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
  (
     num_songs        INTEGER,
     artist_id        VARCHAR,
     artist_latitude  FLOAT,
     artist_longitude FLOAT,
     artist_location  VARCHAR,
     artist_name      VARCHAR,
     song_id          VARCHAR,
     title            VARCHAR,
     duration         FLOAT,
     year             INTEGER
  ) 
""")

songplay_table_create = ("""
CREATE TABLE songplays
  (
     songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
     start_time  TIMESTAMP SORTKEY DISTKEY,
     user_id     INTEGER NOT NULL,
     level       VARCHAR,
     song_id     VARCHAR,
     artist_id   VARCHAR,
     session_id  INTEGER,
     location    VARCHAR,
     user_agent  VARCHAR
  ) 
""")

user_table_create = ("""
CREATE TABLE users
  (
     user_id    INTEGER PRIMARY KEY SORTKEY,
     first_name VARCHAR,
     last_name  VARCHAR,
     gender     VARCHAR,
     level      VARCHAR
  ) 
""")

song_table_create = ("""
CREATE TABLE songs
  (
     song_id   VARCHAR PRIMARY KEY SORTKEY,
     title     VARCHAR,
     artist_id VARCHAR,
     year      INTEGER,
     duration  FLOAT
  ) 
""")

artist_table_create = ("""
CREATE TABLE artists
  (
     artist_id VARCHAR PRIMARY KEY SORTKEY,
     NAME      VARCHAR,
     location  VARCHAR,
     latitude  FLOAT,
     longitude FLOAT
  ) 
""")

time_table_create = ("""
CREATE TABLE time
  (
     start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
     hour       INTEGER,
     day        INTEGER,
     week       INTEGER,
     month      INTEGER,
     year       INTEGER,
     weekday    VARCHAR
  ) 
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
            (
                start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent
            )
SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
                se.userid AS user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionid AS session_id,
                se.location,
                se.useragent AS user_agent
FROM            staging_events AS se
JOIN            staging_songs AS ss
ON           (
                se.artist = ss.artist_name
AND             se.length = ss.duration
AND             se.song = ss.title
             )
WHERE           se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
             (
                user_id,
                first_name,
                last_name,
                gender,
                level
             )
SELECT DISTINCT userid AS user_id,
                firstname AS first_name,
                lastname AS last_name,
                gender,
                level
FROM   staging_events
WHERE  userid IS NOT NULL 
""")

song_table_insert = ("""
INSERT INTO songs
            (
                song_id,
                title,
                artist_id,
                year,
                duration
            )
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM   staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
            (
                artist_id,
                NAME,
                location,
                latitude,
                longitude
            )
SELECT DISTINCT artist_id,
                artist_name AS artist_id,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
FROM   staging_songs 
""")

time_table_insert = ("""
INSERT INTO time
            (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday
            )
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                EXTRACT(hour FROM start_time) AS hour,
                EXTRACT(day FROM start_time) AS day,
                EXTRACT(week FROM start_time) AS week,
                EXTRACT(month FROM start_time) AS month,
                EXTRACT(year FROM start_time) AS year,
                TO CHAR(start_time, 'Day') AS weekday
FROM            staging_events
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]