import spotipy
import pandas as pd
import logging
import os
from aws_connections.s3_bucket import upload_file_to_s3
from snowflake.snowflake_connection import s3_to_snowflake
from dotenv import load_dotenv, find_dotenv
from spotipy.oauth2 import SpotifyOAuth
from spotipy.client import Spotify
from aws_connections.s3_to_redshift import s3_to_redshift
from pathlib import Path

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('.env')



try:
    CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
    CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
    REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI')
    file_path = os.getenv('FILE_PATH')

except KeyError as e:
    print(f"Environment variable {e} not set. Please check your .env file.")
    raise



sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                               client_secret=CLIENT_SECRET,
                                               redirect_uri=REDIRECT_URI,
                                               scope="user-read-recently-played"))
def get_spotify_connectiion() -> Spotify:
    """Returns a Spotify connection object."""
    try:
        connection = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                               client_secret=CLIENT_SECRET,
                                               redirect_uri=REDIRECT_URI,
                                               scope="user-read-recently-played"))
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to Spotify API: {e}")
        raise


def get_recently_played_items(connection: Spotify,limit: int) -> list:
    results = get_spotify_connectiion().current_user_recently_played(limit)
    recently_played_items = [
        {
            'Artist(s)': ", ".join(item['name'] for item in item['track']['artists']),
            'Track Name': item['track']['name'],
            'Album': item['track']['album']['name'],
            'Played At': item['played_at']
        }
        for item in results['items']
    ]
    return recently_played_items

def clean_dataframe(items:list) -> pd.DataFrame:
    df = pd.DataFrame(items, index=range(1, len(items) + 1))
    df['Played At'] = pd.to_datetime(df['Played At'])
    df['Played At'] = df['Played At'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df

def save_to_csv(df: pd.DataFrame, filepath: str) -> None:
    try: 
        df.to_csv(filepath, index=False)
        logging.info(f'Data saved to {filepath}')
    except FileNotFoundError as e:
        logging.error(f"Filepath not found: {e}")
        raise

def main():
    try:
        df = clean_dataframe(get_recently_played_items(sp, 50))
        save_to_csv(df, file_path)
        upload_file_to_s3(file_path)
        s3_to_snowflake()
        s3_to_redshift()
        logging.info("Data sent to Redshift successfully")
        logging.info("Successfully uploaded file to S3")
        logging.info("Successfully copied data to Snowflake table")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


    
if __name__ == "__main__":
    main()