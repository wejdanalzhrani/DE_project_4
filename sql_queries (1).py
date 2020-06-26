import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
                                    artist              VARCHAR,
                                    auth                VARCHAR,
                                    firstName           VARCHAR,
                                    gender              VARCHAR,
                                    itemInSession       INTEGER,
                                    lastName            VARCHAR,
                                    length              FLOAT,
                                    level               VARCHAR,
                                    location            VARCHAR,
                                    method              VARCHAR,
                                    page                VARCHAR,
                                    registration        FLOAT,
                                    sessionId           INTEGER,
                                    song                VARCHAR,
                                    status              INTEGER,
                                    ts                  TIMESTAMP,
                                    userAgent           VARCHAR,
                                    userId              INTEGER 
                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                    num_songs           INTEGER,
                                    artist_id           VARCHAR,
                                    artist_latitude     FLOAT,
                                    artist_longitude    FLOAT,
                                    artist_location     VARCHAR,
                                    artist_name         VARCHAR,
                                    song_id             VARCHAR,
                                    title               VARCHAR,
                                    duration            FLOAT,
                                    year                INTEGER
                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
                                songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
                                start_time          TIMESTAMP       NOT NULL,
                                user_id             INTEGER         NOT NULL,
                                level               VARCHAR,
                                song_id             VARCHAR         NOT NULL,
                                artist_id           VARCHAR         NOT NULL,
                                session_id          INTEGER,
                                location            VARCHAR,
                                user_agent          VARCHAR
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id             INTEGER      NOT NULL    PRIMARY KEY ,
                            first_name          VARCHAR,
                            last_name           VARCHAR,
                            gender              VARCHAR,
                            level               VARCHAR
                            )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
                            song_id             VARCHAR       NOT NULL    PRIMARY KEY,
                            title               VARCHAR ,
                            artist_id           VARCHAR ,
                            year                INTEGER ,
                            duration            FLOAT
                        )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
                            artist_id           VARCHAR        NOT NULL     PRIMARY KEY,
                            name                VARCHAR ,
                            location            VARCHAR,
                            latitude            FLOAT,
                            longitude           FLOAT
                        )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time          TIMESTAMP       NOT NULL   PRIMARY KEY,
                            hour                INTEGER,
                            day                 INTEGER,
                            week                INTEGER,
                            month               INTEGER,
                            year                INTEGER,
                            weekday             VARCHAR(20)
                        )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time,user_id,level,song_id,
                                                 artist_id,session_id,location,user_agent)
					      SELECT DISTINCT e.ts AS start_time,
                          e.userId AS user_id,
                          e.level AS level,
                          s.song_id AS song_id,
                          s.artist_id AS artist_id,
                          e.sessionId as session_id,
                          e.location AS location,
                          e.userAgent AS user_agent
                          FROM staging_events e join staging_songs s ON
                          (e.artist = s.artist_name AND 
                          e.song = s.title AND 
                          e.length= s.duration)
                          WHERE e.page='NextSong'
                          
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name,last_name, gender,level )
					      SELECT DISTINCT userId,
                          firstname,
                          lastname,
                          gender,
                          level
                          FROM staging_events
                          WHERE page='NextSong'
""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration)
					       SELECT DISTINCT song_id,
                           title,
                           artist_id,
                           year,
                           duration
                           FROM staging_songs
                           
""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
					       SELECT DISTINCT artist_id,
                           artist_name as name,
                           artist_location as location,
                           artist_latitude as latitude,
                           artist_longitude as longitude
                           FROM staging_songs
                           
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week , month,year , weekday)
					       SELECT DISTINCT ts,
                           EXTRACT(hour from ts) as hour,
                           EXTRACT(day from ts) as day,
                           EXTRACT(week from ts) as week,
                           EXTRACT(month from ts) as month,
                           EXTRACT(year from ts) as year,
                           EXTRACT(weekday from ts) as weekday
                           FROM staging_events
                           WHERE page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
