import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import psycopg2.extras as extras


def convert_dataframe(df):
    return list(df.itertuples(index=False, name=None))

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data_tuples = convert_dataframe(df[["song_id","title","artist_id","year","duration"]])
    song_data = list(song_data_tuples)
    
    extras.execute_batch(cur,song_table_insert,song_data,100)
    
    # insert artist record
    dedup_artist_df = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].drop_duplicates()
    artist_data = convert_dataframe(dedup_artist_df)
    extras.execute_batch(cur,artist_table_insert,artist_data)

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'].drop_duplicates(),unit='ms')
    
    # insert time data records
    time_data = zip(df['ts'],t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
#     column_labels = ("start_time","hour","day","week","month","year","weekday")

    extras.execute_batch(cur,time_table_insert,time_data)

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]].drop_duplicates()
    user_data = convert_dataframe(user_df)

    extras.execute_batch(cur,user_table_insert,user_data)
    
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
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
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