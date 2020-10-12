# -*- coding: utf-8 -*-
import sys
import time
import json
import base64
import requests
import pandas as pd
import logging

client_id = '67efb4481c914d1c8e8df5120a48cc85'
client_secret = '6f65f400c3c34159a344b87d98ad1314'


def main():
    artist_list = pd.read_csv('artist_list.csv', header = 'artist')
    artistID = [get_aritistID(artist) for artist in artist_list]
    print(artistID)


def get_API(url, *args):
    response = requests.get(url, params = params, headers = headers)

    try:
        response.json()
        return response
    except:
        return get_API(url, *args)
        


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


def get_aritistID():
    url = 'https://api.spotify.com/v1/search'
    headers = get_headers

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

        
    artistID = json.loads(response.text)['artists']['items'][0]['id']
    return artistID

def __main__():
    main()
