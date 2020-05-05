class SqlQueries:
    songplay_table_insert = (""" SELECT
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

    user_table_insert = (""" SELECT distinct 
                    userid, 
                    firstname, 
                    lastname, 
                    gender, 
                    level
            FROM staging_events
            WHERE page='NextSong'
    """)

    song_table_insert = (""" SELECT distinct 
                    song_id, 
                    title, 
                    artist_id, 
                    year, 
                    duration
            FROM staging_songs
    """)

    artist_table_insert = (""" SELECT distinct 
                    artist_id, 
                    artist_name, 
                    artist_location, 
                    artist_latitude, 
                    artist_longitude
            FROM staging_songs
    """)

    time_table_insert = (""" SELECT 
                    start_time, 
                    extract(hour from start_time), 
                    extract(day from start_time), 
                    extract(week from start_time), 
                    extract(month from start_time),
                     extract(year from start_time), 
                     extract(dayofweek from start_time)
            FROM songplays
    """)

    has_rows_songplays = (""" SELECT COUNT(*) FROM songplays""")
    has_null_songplays = (""" SELECT COUNT(*) FROM songplays WHERE playid is NULL""")
    
    has_rows_artists = (""" SELECT COUNT(*) FROM artists""")
    has_null_artists = (""" SELECT COUNT(*) FROM artists WHERE artistid is NULL""")

    has_rows_songs = (""" SELECT COUNT(*) FROM songs""")
    has_null_songs = (""" SELECT COUNT(*) FROM songs WHERE songid is NULL""")

    has_rows_time = (""" SELECT COUNT(*) FROM time""")
    has_null_time = (""" SELECT COUNT(*) FROM time WHERE start_time is NULL""") 

    has_rows_users = (""" SELECT COUNT(*) FROM users""")
    has_null_users = (""" SELECT COUNT(*) FROM users WHERE userid is NULL""") 



    has_rows =  (""" SELECT COUNT(*) FROM """)

""""
sql_template = "" SELECT * FROM {} WHERE {} IS NULL""
myTask1 = YourOperator(
    task_id='templated_one',
    sql=sql_template.format("table1", "col1"),
    dag=dag,
)
"""
