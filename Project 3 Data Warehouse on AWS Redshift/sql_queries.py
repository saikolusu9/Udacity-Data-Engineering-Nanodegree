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

#create events staging table
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events 
(event_id int IDENTITY(0,1) NOT NULL,
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession int,
lastName varchar,
length varchar,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionId BIGINT,
song varchar,
status varchar,
ts BIGINT,
useragent varchar,
userId int
)
""")

#create songs staging table
staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs 
(num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int)
""")

#create song play table
songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays
(songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
start_time BIGINT REFERENCES time, 
user_id varchar REFERENCES users, 
level varchar, 
song_id varchar REFERENCES songs, 
artist_id varchar REFERENCES artists, 
session_id varchar NOT NULL, 
location varchar, 
user_agent varchar)
""")

#create users table
user_table_create = (""" CREATE TABLE IF NOT EXISTS users
(user_id text PRIMARY KEY, 
first_name varchar NOT NULL, 
last_name varchar NOT NULL, 
gender varchar NOT NULL, 
level varchar
)
""")

#create songs table
song_table_create = (""" CREATE TABLE IF NOT EXISTS songs
(song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year int , 
duration float NOT NULL
)
""")

#create artists table
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, 
location varchar, 
latitude float, 
longitude float)
""")

#create time table
time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(start_time timestamp PRIMARY KEY, 
hour int NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday int NOT NULL)
""")

# STAGING TABLES

#copy into events staging table
staging_events_copy = ("""copy staging_events 
from {}
iam_role {}
JSON 'auto ignorecase'
region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'))

#copy into songs staging table
staging_songs_copy = ("""copy staging_songs 
from {}
iam_role {}
JSON 'auto ignorecase'
region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

#insert into songplay table
songplay_table_insert = (""" INSERT INTO songplays 
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT DISTINCT a.ts, a.userId, a.level, b.song_id, c.artist_id, a.sessionId, a.location, a.userAgent 
FROM 
staging_events a
join songs b on a.song = b.title
join artists c on a.artist = c.name
where a.page='NextSong')
""")

#insert into users table
user_table_insert = (""" INSERT INTO users 
(user_id, first_name, last_name, gender, level)
(SELECT DISTINCT userId, firstName, lastName, gender, level 
FROM staging_events
WHERE page = 'NextSong'
and userId not in (Select distinct user_id from users)) 
""")

#insert into songs table
song_table_insert = (""" INSERT INTO songs 
(song_id, title, artist_id, year, duration)
(Select DISTINCT song_id, title, artist_id, year, duration 
FROM 
staging_songs
where song_id not in (Select distinct song_id from songs)) 
""")

#insert into artists table
artist_table_insert = (""" INSERT INTO artists 
(artist_id, name, location, latitude, longitude)
(Select DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM
staging_songs
where artist_id not in (Select distinct artist_id from artists))
""")

#insert into time table
time_table_insert = (""" INSERT INTO time 
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
a.start_time as start_time,
EXTRACT (HOUR FROM a.start_time) as hour,
EXTRACT (DAY FROM a.start_time) as day,
EXTRACT (WEEK FROM a.start_time) as week,
EXTRACT (MONTH FROM a.start_time) as month,
EXTRACT (YEAR FROM a.start_time) as year,
EXTRACT (WEEKDAY FROM a.start_time) as weekday
from
(SELECT TIMESTAMP 'epoch' + se.ts * (INTERVAL '1 second' / 1000) as start_time 
from staging_events se
where se.page='NextSong') a;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
