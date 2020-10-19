import os
import csv
import sys
import json
import pymysql
import spotify_api as api
from tqdm import tqdm

global client_id
global client_secret

def main():
    with open(os.path.join('..', 'secret.json'), 'r') as f:
        secret = json.load(f)

    client_id = secret['Spotify']['client_id']
    client_secret = secret['Spotify']['client_secret']

    host = secret['Spotify']['host']
    port = secret['Spotify']['port']
    username = secret['Spotify']['username']
    database = secret['Spotify']['database']
    pw = secret['Spotify']['pw']

    # DB 접속
    try:
        conn = pymysql.connect(host, user = username, passwd = pw, db = database, port = port,
                           use_unicode = True, charset = 'utf8')
        cursor = conn.cursor()
    except:
        sys.exit(1)

    # api 수집 artists 목록
    artists = []
    with open('artist_list.csv') as f:
        line = csv.reader(f)
        for l in line:
            artists.append(l[0])
    
    # api 수집
    for artist in tqdm(artists):
        res = api.get_artistInfo(artist)
        if res != 'ID error':

            genres_list = [{'artist_id' : res['id'], 'genre' : genre} for genre in res['genres'].split('|')]

            for data in genres_list:
                insertQue(cursor, data, 'artist_genres')

            del res['genres']
            insertQue(cursor, res, 'artists')

    conn.commit()
    cursor.close()

# INSERT
def insertQue(cursor, data, table):
    columns = ', '.join(data.keys()) # table columns
    placeholders = ', '.join(['%s'] * len(data)) # values
    key_holders = ', '.join([k + '=%s' for k in data.keys()])
    que = "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(table, columns, placeholders, key_holders)
    cursor.execute(que, list(data.values())*2)

# DELETE

if __name__ == '__main__':
    main()

