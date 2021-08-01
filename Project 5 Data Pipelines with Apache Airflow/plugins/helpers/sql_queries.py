class SqlQueries:
    
    songplay_table_insert = ("""
        INSERT INTO public.songplays (
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent
        )
        SELECT DISTINCT 
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM public.staging_events
        WHERE page='NextSong') events
        LEFT JOIN public.staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
    """)


    song_table_insert = ("""
        INSERT INTO public.songs (
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        )
        Select DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration 
        FROM 
            public.staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO public.artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude
        )
        Select DISTINCT 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM
            public.staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO public.time (
            start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday
        )
        SELECT DISTINCT
            a.start_time as start_time,
            EXTRACT (HOUR FROM a.start_time) as hour,
            EXTRACT (DAY FROM a.start_time) as day,
            EXTRACT (WEEK FROM a.start_time) as week,
            EXTRACT (MONTH FROM a.start_time) as month,
            EXTRACT (YEAR FROM a.start_time) as year,
            EXTRACT (WEEKDAY FROM a.start_time) as weekday
        FROM
            (SELECT TIMESTAMP 'epoch' + se.ts * (INTERVAL '1 second' / 1000) as start_time 
            FROM public.staging_events se
            WHERE se.page='NextSong') a;
    """)
    
    users_table_insert = ("""
        INSERT INTO public.users (
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level
        ) 
        SELECT DISTINCT 
            userId, 
            firstName, 
            lastName, 
            gender, 
            level 
        FROM 
            public.staging_events 
        WHERE 
            page = 'NextSong'
    """)