import sys
import os
import logging
import boto3
import requests
import base64
import json
import pymysql
from datetime import datetime
import pandas as pd
import jsonpath
import spotify_api as api
import spotify_rdb as rdb

global client_id
global client_secret

with open(os.path.join('..', 'secret.json'), 'r') as f:
    secret = json.load(f)

client_id = secret['Spotify']['client_id']
client_secret = secret['Spotify']['client_secret']

# db_params = {
#     'host' : secret['Spotify']['host'],
#     'port' : secret['Spotify']['port'],
#     'username' : secret['Spotify']['username'],
#     'database' : secret['Spotify']['database'],
#     'pw' : secret['Spotify']['pw']
# }

db_params = {
    'host' : 'moondb.csctoaeodhiy.ap-northeast-2.rds.amazonaws.com',
    'port' : 3306,
    'username' : 'admin',
    'database' : 'production',
    'pw' : '12345678'
}

aws_id = secret['AWS']['ID']
aws_key = secret['AWS']['KEY']

def main():

    # DB 접속
    conn, cursor = rdb.connectDB(**db_params)

    headers = api.get_headers(client_id, client_secret)

    #  RDS DB 조회 - artist 모든 id 조회
    cursor.execute("SELECT id FROM artists LIMIT 10")
    artist_ids = [i for (i,) in cursor.fetchall()]
    conn.commit()
    cursor.close()

    top_track_keys = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify"
    }

    # 앨범 track 정보 수집
    top_tracks = []
    params = {'country' : 'US'}

    for artist_id in artist_ids:
        tracks = api.get_tracks(artist_id, params = params)

        for track in tracks:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k: jsonpath.jsonpath(i, v)})
                top_track.update({'artist_id': id})
                top_tracks.append(top_track)

    # track_ids
    track_ids = [i['id'][0] for i in top_tracks]

    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('top-tracks.parquet',
                            engine = 'pyarrow',
                            compression = 'snappy'
                            )

    dt = datetime.utcnow().strftime("%Y-%m-%d")

    s3 = boto3.resource('s3',
                        region_name = 'ap-northeast-2',
                        aws_access_key_id = aws_id,
                        aws_secret_access_key = aws_key
                        )
    ob = s3.Object('spotify-artists', 'top-tracks/dt={}/top-tracks.parquet'.format(dt))
    data = open('top-tracks.parquet', 'rb')
    ob.put(Body = data)

    # S3 import
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    audio_features = []

    for i in tracks_batch:
        ids = ','.join(i)
        audio = api.get_audio(ids)
        audio_features.extend(audio)

    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet('audio-features.parquet',
                                engine = 'pyarrow',
                                compression = 'snappy'
                                )

    s3 = boto3.resource('s3',
                        region_name = 'ap-northeast-2',
                        aws_access_key_id = aws_id,
                        aws_secret_access_key = aws_key
                        )
    ob = s3.Object('spotify-artists', 'audio-features/dt={}/audio_features.parquet'.format(dt))
    data = open('audio-features.parquet', 'rb')
    ob.put(Body = data)

if __name__=='__main__':
    main()