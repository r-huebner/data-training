class SqlQueries:

    copy_sql = """
    COPY {table}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS JSON '{json_path}';
    """

    staging_events_table_create =("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
    """)
    staging_songs_table_create =("""
        CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR,
        num_songs INT,
        title VARCHAR,
        artist_name VARCHAR(500),
        artist_latitude FLOAT,
        year INT,
        duration FLOAT,
        artist_id VARCHAR,
        artist_longitude FLOAT,
        artist_location VARCHAR(500)
    );
    """)

    songplays_table_create=("""
        CREATE TABLE IF NOT EXISTS songplays (
        songplay_id VARCHAR PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        userid INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        sessionid INT,
        location VARCHAR,
        useragent VARCHAR
    );
    """)

    users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        userid INT PRIMARY KEY,
        firstname VARCHAR,
        lastname VARCHAR,
        gender CHAR(1),
        level VARCHAR
        );
    """)

    songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            artist_id VARCHAR,
            year INT,
            duration FLOAT
        );
    """)

    artists_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY,
            artist_name VARCHAR(500),
            artist_location VARCHAR(500),
            artist_latitude FLOAT,
            artist_longitude FLOAT
        );
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        );
    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (
            songplay_id,
            start_time,
            userid,
            level,
            song_id,
            artist_id,
            sessionid,
            location,
            useragent
        )
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO "users" (userid, firstname, lastname, gender, level)
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page = 'NextSong';
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO "time" (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)