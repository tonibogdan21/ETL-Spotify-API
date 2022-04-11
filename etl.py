import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

# This code will take specific data from Spotify API and load it into a sqlite database

DATABASE_LOCATION = "sqlite:///my_songs.sqlite"
USER_ID = "Antonio Bogdan"
TOKEN = "BQAhM0f0m3G3DCNfYU424Ihyl5e9DJUmScSuBw_TMCImkpmJMEzPUWOQSajWwGozZzP0pSXFWgreolIzLX4eqm28dEhVz2t4o3mm3ccL3iTymzN-4AKBOI0A_KP15NskSB2TI3b_L5Z4F3b9LeBY_ScoKarv9GwY9HyxoOVt"

# Validation
def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs listened to in the last 24 hrs.")
        return False

    # Primary key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check went wrong.")

    if df.isnull().values.any():
        raise Exception("Null values found.")

if __name__ == '__main__':

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

# Extract the data from the last 24 hours
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

# Made an empty list for each column i'll use in the database
    song_names = []
    artists_names = []
    played_at_list = []
    timestamps = []

# The 'data' item returns a big json from the Spotify API and I have to extract the specific data I need
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artists_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artists_names,
        "played_at" : played_at_list,
        "timestamps" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamps"])

    if check_if_valid_data(song_df):
        print("Data looks good, proceed to Load.")

#Load
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect("my_played_tracks.sqlite")
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamps VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Opened database.")

try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
except:
    print("Data already exists in the database.")

conn.close()
print("Closed database.")
