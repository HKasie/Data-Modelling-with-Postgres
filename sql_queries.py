
# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    
user_table_drop = "DROP TABLE IF EXISTS users"

song_table_drop = "DROP TABLE IF EXISTS songs"

artist_table_drop = "DROP TABLE IF EXISTS artists"
    
time_table_drop = "DROP TABLE IF EXISTS time"
 
    
    
    
# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                 songplay_id serial PRIMARY KEY, 
                 start_time timestamp NOt NULL, 
                 user_id varchar NOT NULL, 
                 level varchar,
                 song_id varchar, 
                 artist_id varchar,
                 session_id int,
                 location varchar,
                 user_agent varchar);
                 """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                 user_id varchar DEFAULT NULL PRIMARY KEY, 
                 first_name varchar DEFAULT NULL,
                 last_name varchar DEFAULT NULL,
                 gender varchar DEFAULT NULL,
                 level varchar DEFAULT NULL);
                 """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                 song_id varchar PRIMARY KEY,
                 title varchar,
                 artist_id varchar,
                 year int,
                 duration numeric);
                 """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                 artist_id varchar PRIMARY KEY,
                 name varchar,
                 location varchar,
                 latitude numeric,
                 longitude numeric);
                 """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                 start_time timestamp NOT NULL PRIMARY KEY,
                 hour int, 
                 day int,
                 week int,
                 month int,
                 year int,
                 weekday varchar);
                 """)

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (
                start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent)
                values (%s, %s, %s, %s, %s, %s, %s, %s);
                """)

user_table_insert = (""" INSERT INTO users (
                user_id,
                first_name,
                last_name,
                gender,
                level)
                values (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id) 
                DO UPDATE SET level = excluded.level; 
                """)

song_table_insert = (""" INSERT INTO songs (
                song_id,
                title,
                artist_id,
                year,
                duration)
                values (%s, %s, %s, %s, %s);
                """)

artist_table_insert = (""" INSERT INTO artists (
                artist_id,
                name,
                location,
                latitude,
                longitude)
                values (%s, %s, %s, %s, %s);
                """)


time_table_insert = (""" INSERT INTO time (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday)
                values (%s, %s, %s, %s, %s, %s, %s);
                """)

# FIND SONGS

song_select = ("""SELECT song_id, artists.artist_id
               FROM songs
               INNER JOIN artists 
               ON songs.artist_id = artists.artist_id 
               WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
               """)

# QUERY LISTS

create_table_queries = [songplay_table_create, 
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
                        


drop_table_queries = [songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
                      