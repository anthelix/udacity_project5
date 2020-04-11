class CreateTables:
    staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_events
    (
        artist            varchar(256),
        auth              varchar(45),
        firstName         varchar(50),
        gender            varchar(1),
        itemInSession     smallint ,
        lastName          varchar(50),
        length            float,
        level             varchar(10),
        location          varchar(256),
        method            varchar(10),
        page              varchar(50),
        registration      float,
        sessionId         varchar(256), 
        song              varchar(256), 
        status            smallint, 
        ts                bigint,
        userAgent         varchar(1024),
        userId            int
    );
    """)

    staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_songs
    (
        num_songs         bigint, 
        artist_id         varchar(20), 
        artist_latitude   float, 
        artist_longitude  float, 
        artist_location   varchar(256),  
        artist_name       varchar(256), 
        song_id           varchar(20),
        title             varchar(256),  
        duration          float, 
        year              int
    );
    """)


    ## Dimension Tables
    user_table_create = ("""CREATE TABLE IF NOT EXISTS public.dimUser
    (
        user_id bigint NOT NULL PRIMARY KEY SORTKEY,
        first_name varchar,
        last_name varchar,
        gender varchar(1),
        level varchar NOT NULL
    );
    """)

    song_table_create = ("""CREATE TABLE IF NOT EXISTS public.dimSong
    (
        song_id varchar NOT NULL PRIMARY KEY SORTKEY,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year int,
        duration numeric
    );
    """)

    artist_table_create = ("""CREATE TABLE IF NOT EXISTS public.dimArtist
    (
        artist_id varchar NOT NULL PRIMARY KEY SORTKEY,
        name varchar NOT NULL,
        location varchar,
        latitude numeric,
        longitude numeric
    );
    """)

    time_table_create = ("""CREATE TABLE IF NOT EXISTS public.dimTime
    (
        start_time timestamp NOT NULL PRIMARY KEY DISTKEY SORTKEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar
    );
    """)


    ## Fact Tables
    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS public.factSongplay
    (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL DISTKEY,
        user_id bigint NOT NULL ,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar SORTKEY,
        session_id varchar NOT NULL,
        location varchar,
        user_agent varchar
    );
    """)


