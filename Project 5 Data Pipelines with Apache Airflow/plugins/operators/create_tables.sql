CREATE TABLE IF NOT EXISTS public.staging_events (
	event_id int IDENTITY(0,1) NOT NULL,
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
);

CREATE TABLE IF NOT EXISTS public.staging_songs (
	num_songs int,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
);


CREATE TABLE IF NOT EXISTS public.artists (
	artist_id varchar PRIMARY KEY, 
	name varchar NOT NULL, 
	location varchar, 
	latitude float, 
	longitude float
);


CREATE TABLE IF NOT EXISTS public.songs (
	song_id varchar PRIMARY KEY, 
	title varchar NOT NULL, 
	artist_id varchar NOT NULL, 
	year int , 
	duration float NOT NULL
);

CREATE TABLE IF NOT EXISTS public.users (
	user_id text PRIMARY KEY, 
	first_name varchar NOT NULL, 
	last_name varchar NOT NULL, 
	gender varchar NOT NULL, 
	level varchar
);


CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
);

CREATE TABLE IF NOT EXISTS public.songplays (
	songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
	start_time timestamp REFERENCES public."time", 
	user_id varchar REFERENCES public.users, 
    level varchar, 
    song_id varchar REFERENCES public.songs, 
    artist_id varchar REFERENCES public.artists, 
    session_id varchar NOT NULL, 
    location varchar, 
    user_agent varchar
);

