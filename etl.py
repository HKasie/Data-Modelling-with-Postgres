import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Processes the song file using the filepath argument provided.
    It extracts the information on songs to be stored in the songs table.
    Also extracts the information on artists to be stored in the artist table.
    
    Inputs
    cur: postgres cursor
    filepath: the filepath to directory of the song file
    
    Returns: 
    None
    """
    
    #open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    Processes the log file using the filepath argument provided.
    Extracts datetime information to be stored in the time table.
    Extracts songs played to be stored in songplay table.
    Song ID and Artist ID are required to get songs played.
    Song ID and Artist ID are are not include in logfile.
    Use song_select query in sql_queries.py to get song ID and artist ID.
         
    Inputs:
    cur: postgres cursor
    filepath: the filepath to directory of the log file
    
    Returns: 
    None
    """
     
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df['page'] == 'NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = pd.concat([df['ts'],
                t.dt.hour,
                t.dt.day,
                t.dt.week,
                t.dt.month,
                t.dt.year,
                t.dt.weekday],
                axis = 1)
              
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data = time_data.values, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = ( pd.to_datetime(row.ts, unit='ms'), 
                     int(row.userId), 
                     row.level, 
                     songid, 
                     artistid, 
                     row.sessionId, 
                     row.location, 
                     row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    
    """ 
    This funtion processes sparkify data
    
    Inputs: 
    cur: postgres cursor
    conn: postgres connection
    filepath: file path to the directory of files to be processed
    func: this is the name of the function for processing file types
  
    Returns: 
    None
    """
   
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()