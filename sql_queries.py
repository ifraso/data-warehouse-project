import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN    = config.get('IAM_ROLE', 'arn')
SONG_DATA       = config.get('S3', 'song_data')
LOG_DATA        = config.get('S3', 'log_data')
LOG_JSONPATH    = config.get('S3', 'log_jsonpath')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR(400),
        auth                VARCHAR(20),
        firstName           VARCHAR(50),
        gender              CHAR(1),
        itemInSession       SMALLINT,
        lastName            VARCHAR(50),
        lenght              FLOAT,
        level               VARCHAR(10),
        location            VARCHAR(200),
        method              VARCHAR(20),
        page                VARCHAR(20),
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR(400),
        status              SMALLINT,
        ts                  BIGINT,
        userAgent           VARCHAR(200),
        userId              INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           SMALLINT,
        artist_id           VARCHAR(20),
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR(300),
        artist_name         VARCHAR(400),
        song_id             VARCHAR(20),
        title               VARCHAR(200),
        duration            FLOAT,
        year                SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time          TIMESTAMP NOT NULL SORTKEY,
        user_id             INT NOT NULL,
        level               VARCHAR(10) NOT NULL,
        song_id             VARCHAR(20) NOT NULL DISTKEY,
        artist_id           VARCHAR(20) NOT NULL,
        session_id          INT NOT NULL,
        location            VARCHAR(200) NOT NULL,
        user_agent          VARCHAR(200) NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id             INT NOT NULL SORTKEY PRIMARY KEY,
        first_name          VARCHAR(50) NOT NULL,
        last_name           VARCHAR(50) NOT NULL,
        gender              CHAR(1),
        level               VARCHAR(10) NOT NULL
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR(20) NOT NULL DISTKEY PRIMARY KEY,
        title               VARCHAR(200) NOT NULL SORTKEY,
        artist_id           VARCHAR(20) NOT NULL,
        year                SMALLINT,
        duration            FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR(20) NOT NULL PRIMARY KEY,
        name                VARCHAR(400) NOT NULL SORTKEY,
        location            VARCHAR(300),
        latitude            FLOAT,
        longitude           FLOAT
    )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP NOT NULL SORTKEY PRIMARY KEY,
        hour                SMALLINT NOT NULL,
        day                 SMALLINT NOT NULL,
        week                SMALLINT NOT NULL,
        month               SMALLINT NOT NULL,
        year                SMALLINT NOT NULL,
        weekday             SMALLINT NOT NULL
    )
    DISTSTYLE ALL;
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM '{}/2018'
    IAM_ROLE '{}'
    JSON '{}'
    COMPUPDATE OFF 
    REGION 'us-west-2';
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM '{}/A'
    IAM_ROLE '{}'
    JSON 'auto'
    COMPUPDATE OFF
    REGION 'us-west-2';
""").format(SONG_DATA, IAM_ROLE_ARN)


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts * INTERVAL '0.001 seconds', se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events AS se JOIN staging_songs AS ss ON se.song = ss.title
    WHERE page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userId, firstName, lastName, NULLIF(gender, ''), level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, NULLIF(year, '0'), NULLIF(duration, '0')
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, NULLIF(artist_location, ''), NULLIF(artist_latitude, '0'), NULLIF(artist_longitude, '0')
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT TIMESTAMP 'epoch' + ts * INTERVAL '0.001 seconds' AS timestamp, DATE_PART(h, timestamp), DATE_PART(d, timestamp),
        DATE_PART(w, timestamp), DATE_PART(mon, timestamp), DATE_PART(y, timestamp), DATE_PART(dw, timestamp)
    FROM staging_events
    WHERE page = 'NextSong';
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]