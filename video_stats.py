
import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='./.env')
from datetime import date
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

def get_video_data(videos_ids):

    extract_data = []

    def batch_list(video_id_list, batch_size):
        for video_id in range(0,len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size]
    try:
        for batch in batch_list(videos_ids, MAX_RESULT):
            videos_ids_str = "," .join(batch)

            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={videos_ids_str}&key={API_KEY}'
            response = requests.get(url)   # <-- MOVED OUTSIDE if
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                video_data = {
                    'video_id': video_id,
                    'title': snippet['title'],
                    'publishedAt': snippet['publishedAt'],
                    'duration': contentDetails['duration'],
                    'viewCount': statistics.get('viewCount', None),
                    'likeCount': statistics.get('likeCount', None),
                    'commentCount': statistics.get('commentCount', None),
                }
                extract_data.append(video_data)
        return extract_data
    except requests.exceptions.RequestException as e:
        raise e


def data_save_to_json (extract_data):
    file_path=f'./data/YT_data{date.today()}.json'
    with open (file_path,'w', encoding='utf-8') as json_outfile:
        json.dump(extract_data, json_outfile, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    playListId = get_playList_Id()
    videos_ids = get_videos_ids(playListId)
    video_data= get_video_data(videos_ids)
    data_save_to_json(video_data)