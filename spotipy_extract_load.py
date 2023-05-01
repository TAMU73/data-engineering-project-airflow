import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import json
import os
from dotenv import load_dotenv
import boto3


load_dotenv()

def upload_to_s3():

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%dT%H-%M-%S")
    s3 = session.resource('s3')
    s3.meta.client.upload_file(Filename='sample_tracks.csv', Bucket='project-1-bucket', Key=f'sample_tracks_airflow/{dt_string}.csv')

def run_project_etl():

    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials()) # credentials from .env - auto

    birdy_uri = 'spotify:artist:2WX2uTcsvV5OnS0inACecP'
    results = sp.artist_top_tracks(birdy_uri)
    tracks = results['tracks']
    # print(tracks)

    track_list = []
    for track in tracks:
        # print(type(track))
        refined_track = {
            "name": track['name'],
            "popularity": track['popularity'],
            "track_number": track['track_number'],
            "type": track['type'],
            "duration_ms": track['duration_ms'],
        }
        track_list.append(refined_track)


    df = pd.DataFrame(track_list)
    df.to_csv("sample_tracks.csv")
