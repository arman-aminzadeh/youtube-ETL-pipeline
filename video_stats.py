
import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='./.env')

API_KEY = os.getenv('API_KEY')
CHANNEL_HANDLE = os.getenv('CHANNEL_HANDLE')
MAX_RESULT = 50
#url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}' 


def get_playList_Id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}' 

        response = requests.get(url)
#        ##data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        data = response.json()
#        #print(json.dumps(data, indent=4))
        channel_items = data['items'][0]
        channel_playListId = channel_items['contentDetails']['relatedPlaylists']['uploads']
#
#        #print(channel_items)
#        #print(channel_playListId)
#
        return channel_playListId
    except requests.exceptions.RequestException as e:
        raise e




#'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=1&pageToken=EAAaHlBUOkNBRWlFRVUxTlVFNVFqZzJRVEF4UXprMU1rRQ&playlistId=UUX6OQ3DkcsbYNE6H8uQQuVA&key=[YOUR_API_KEY]' \


def get_videos_ids(playListId):
        
        base_url =   f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULT}&playlistId={playListId}&key={API_KEY}'
        pageToken = None
        videos_ids = []
        try:
            while True:
                # Always call the API — first page has no pageToken
                url = base_url
                if pageToken:
                    url += f"&pageToken={pageToken}"

                response = requests.get(url)   # <-- MOVED OUTSIDE if
                response.raise_for_status()
                data = response.json()

                for item in data.get('items', []):
                    video_id = item['contentDetails']['videoId']
                    videos_ids.append(video_id)

                pageToken = data.get('nextPageToken')
                if not pageToken:
                    break

            return videos_ids

        except requests.exceptions.RequestException as e:
            raise e
        

if __name__ == '__main__':
    playListId = get_playList_Id()
    get_videos_ids(playListId)