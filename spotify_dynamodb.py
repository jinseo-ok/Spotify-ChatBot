import requests
import json
import base64
import sys
import time
import pymysql
import boto3
import spotify_api as api
import spotify_rdb as rdb

def main():
    with open(os.path.join('..', 'Config.json'), 'r') as f:
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

    # DB 접속
    conn, cursor = rdb.connectDB(**db_params)

    # RDS DB 조회 - artist 모든 id 조회 
    cursor.execute('SELECT id FROM artists')
    artist_ids = [i for (i,) in cursor.fetchall()]
    conn.commit()
    cursor.close()

    # DynamoDB 접속
    dynamodb = connectDynamo()

    table = dynamodb.Table('top_tracks')

    # 앨범 track 정보 수집
    params = {
        'country': 'US'
    }

    for artist_id in artist_ids:
        tracks = api.get_tracks(artist_id, params)

        for track in tracks:
            data = {
                'artist_id' : artist_id,
                'country': params['country']
            }
            data.update(track)
            table.put_item(Item = data)

def connectDynamo():
    try:
        dynamodb = boto3.resource('dynamodb',
                                    region_name = 'ap-northeast-2',
                                    endpoint_url = 'http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        print('DynamoDB connect error')
        sys.exit(1)

    return dynamodb

if __name__ == '__main__':
    main()

    




