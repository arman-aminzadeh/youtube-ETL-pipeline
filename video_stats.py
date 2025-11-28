
import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='./.env')

API_KEY = os.getenv('API_KEY')
CHANNEL_HANDLE = os.getenv('CHANNEL_HANDLE')

def get_playList_Id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}' 

        response = requests.get(url)
        ##data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        data = response.json()
        #print(json.dumps(data, indent=4))
        channel_items = data['items'][0]
        channel_playListId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        #print(channel_items)
        #print(channel_playListId)

        return channel_playListId
    except requests.exceptions.RequestException as e:
        raise e
    

if __name__ == '__main__':
    get_playList_Id()