# -*- coding: utf-8 -*-
import sys
import time
import json
import base64
import requests
import pandas as pd
import logging

global client_id
global client_secret

client_id = ''
client_secret = ''


def main():
    artist_list = pd.read_csv('artist_list.csv', header = None)
    artistID = [get_aritistID(artist) for artist in artist_list[0]]
    print(artistID)


def get_API(url, params = None, headers):
    response = requests.get(url, params = params, headers = headers)

    try:
        response.json()
        return response
    except:
        return get_API(url, params, headers)
        

def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token" # access token을 받기 위한 endpoint
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii') # client_id 와 secret을 utf-8로 인코딩

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    response = requests.post(endpoint, data=payload, headers=headers)   
    data = json.loads(response.text)

    access_token = data['access_token']
    print(access_token)
    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers


def get_aritistID(query):
    url = 'https://api.spotify.com/v1/search'
    headers = get_headers(client_id, client_secret)
    print()

    params = {
        'q' : query,
        'type' : 'artist',
        'limit' : 1
    }

    try:
        # response = requests.get('https://api.spotify.com/v1/search', params = params, headers = headers)
        response = get_API(url, params, headers)
    except:
        logging.error(response.text)
        sys.exit(1)

    if response.status_code != 200:

        if response.status_code == 429:
            retry_time = json.loads(response.headers)['Retry-After']
            time.sleep(int(retry_time))
            response = get_API(url, params, headers)

        elif response.status_code == 401:
            headers = get_headers(client_id, client_secret)
            response = get_API(url, params, headers)

        else:
            print(response.status_code, 'error')
            logging.error(response.text)

        
    artistId = json.loads(response.text)['artists']['items'][0]['id']
    return artistId


def get_artistInfo(query):
    try:
        artistId = get_aritistID(query)
    except:
        print('cant get artist ID')
    
    endpoint = f'https://api.spotify.com/v1/artists/{artistID}'
    headers = get_headers(client_id, client_secret)

    response = get_API(endpoint, headers)
    data = json.loads(response.text)
    


    response = requests.get(endpoint, headers=headers)






if __name__ == '__main__':
    main()
