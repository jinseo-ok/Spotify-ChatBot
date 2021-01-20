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
from tqdm import tqdm

global client_id
global client_secret

with open(os.path.join('..', 'secret.json'), 'r') as f:
    secret = json.load(f)

client_id = secret['Spotify']['client_id']
client_secret = secret['Spotify']['client_secret']

db_params = {
    'host' : secret['Spotify']['host'],
    'port' : secret['Spotify']['port'],
    'username' : secret['Spotify']['username'],
    'database' : secret['Spotify']['database'],
    'pw' : secret['Spotify']['pw']
}


aws_id = secret['AWS']['ID']
aws_key = secret['AWS']['KEY']

def main():

    # DB 접속
    conn, cursor = rdb.connectDB(**db_params)

    headers = api.get_headers(client_id, client_secret)

    # RDS DB 조회 - artist 모든 id 조회
    cursor.execute("SELECT id FROM artists")
    artist_ids = [i for (i,) in cursor.fetchall()]
    conn.commit()
    cursor.close()

    top_track_keys = {
        'id' : 'id',
        'name' : 'name',
        'popularity' : 'popularity',
        'external_url' : 'external_urls.spotify'
    }

    # 앨범 track 정보 수집
    top_tracks = []
    params = {'country' : 'US'}
    print('-------- 앨범 track 정보 수집 --------')
    for artist_id in tqdm(artist_ids):
        tracks = api.get_tracks(artist_id, params = params)

        for track in tracks:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({
                                    k : jsonpath.jsonpath(track, v)
                                    })
                top_track.update({
                                    'artist_id' : artist_id
                                    })
                top_tracks.append(top_track)

    # s3 저장을 위한 parquet화
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
    ob = s3.Object('oo-spotify', 'top-tracks/dt={}/top-tracks.parquet'.format(dt))
    data = open('top-tracks.parquet', 'rb')
    ob.put(Body = data)

    # audio features 정보 수집
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    audio_features = []
    print('-------- audio features 정보 수집 --------')
    for i in tqdm(tracks_batch):
        ids = ','.join(i)
        audio = api.get_audio(ids)
        audio_features.extend(audio)

    # s3 저장을 위한 parquet화
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

    ob = s3.Object('oo-spotify', 'audio-features/dt={}/audio_features.parquet'.format(dt))
    data = open('audio-features.parquet', 'rb')
    ob.put(Body = data)

if __name__=='__main__':
    main()