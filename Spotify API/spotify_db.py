import os
import csv
import sys
import json
import pymysql
import spotify_api as api

global client_id
global client_secret

client_id = secret['Spotify']['client_id']
client_secret = secret['Spotify']['client_secret']

def main():
    with open(os.path.join('..', 'secret.json'), 'r') as f:
        secret = json.load(f)

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
    for artist in artists:
        res = api.get_artistInfo(client_id, client_secret, artist)
        if res != 'ERROR':

            genres_list = [[res['id'], genre] for genre in res['genres'].split('|')]

            for data in genres_list:
                insertQue(cursor, data, 'artist_genres')

            del res['genres']
            insertQue(cursor, res, 'artists')


def insertQue(cursor, data, table):
    col = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    key_holders = ', '.join([k + '=%s' for k in data.keys()])
    que = "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(table, col, placeholders, key_holders)
    cursor.execute(que, list(data.values())*2)



