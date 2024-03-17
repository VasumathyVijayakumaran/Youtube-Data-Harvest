import json
from pymongo import MongoClient
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def load_config(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def save_channel_info_to_mongodb(config, channel_info):
    client = MongoClient(config['mongo']['connection_string'])
    db = client[config['mongo']['db_name']]
    collection = db[config['mongo']['channel_collection']]
    ##collection.insert_one(channel_info)
    # Perform upsert
    collection.update_one({'_id': channel_info['_id']}, {'$set': channel_info}, upsert=True)

def save_video_info_to_mongodb(config, video_inform):
    client = MongoClient(config['mongo']['connection_string'])
    db = client[config['mongo']['db_name']]
    collection = db[config['mongo']['video_collection']]
    ##collection.insert_one(video_info)
    collection.update_one({'_id': video_inform['_id']}, {'$set': video_inform}, upsert=True)

def save_comments_to_mongodb(config, comments):
    if comments:
        for item in comments:
            client = MongoClient(config['mongo']['connection_string'])
            db = client[config['mongo']['db_name']]
            collection = db[config['mongo']['comment_collection']]
            ##collection.insert_many(comments)
            collection.update_one({'_id': item['_id']}, {'$set': item}, upsert=True)
        

def get_channel_info(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    response = youtube.channels().list(
        part='snippet,statistics,contentDetails',
        id=channel_id
    ).execute()
    
    channel_data = response['items'][0]
    channel_info = {
        '_id': channel_data['id'],
        'channelId': channel_data['id'],
        'videoCount': int(channel_data['statistics']['videoCount']),
        'subscriberCount': int(channel_data['statistics']['subscriberCount']),
        'title': channel_data['snippet']['title'],
        'description': channel_data['snippet']['description'],
        'playlistId': channel_data['contentDetails']['relatedPlaylists']['uploads'],
        'channelType': channel_data['kind']
    }
    
    return channel_info

def get_video_ids(api_key, channel_id):
    video_url = 'https://www.googleapis.com/youtube/v3/search'
    video_params = {
        'part': 'snippet',
        'channelId': channel_id,
        'type': 'video',
        'maxResults': 1000,
        'key': api_key,
        }

    all_video_ids = []

    while True:
        response = requests.get(video_url, params=video_params)
        data = response.json()

        try:
            items = data['items']
            video_ids = [item['id']['videoId'] for item in items]
            all_video_ids.extend(video_ids)

            # Check if there are more pages
            if 'nextPageToken' in data:
                video_params['pageToken'] = data['nextPageToken']
            else:
                break

        except KeyError as e:
            print(f"KeyError: {e}")
            print("Check the structure of the API response.")
            return []
    return all_video_ids


def get_videoinfo_from_video(api_key, video_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
               
    response = youtube.videos().list(
    part="snippet,contentDetails,statistics",
    id=video_id).execute()
    video_data=response['items'][0]
     
    video_info = {
                '_id': video_id,
                'channelId': video_data['snippet']['channelId'],
                'videoId': video_id,
                'videoName': video_data['snippet']['title'],
                'videoDescription': video_data['snippet']['description'],
                'thumbnailUrl': video_data['snippet']['thumbnails']['default']['url'],
                'publishedAt': video_data['snippet']['publishedAt'],
                "viewCount": int(video_data["statistics"].get("viewCount", 0)),
                "likeCount": int(video_data["statistics"].get("likeCount", 0)),
                "dislikeCount": int(video_data["statistics"].get("dislikeCount", 0)),
                "favoriteCount": int(video_data["statistics"].get("favoriteCount", 0)),
                "commentCount": int(video_data["statistics"].get("commentCount", 0)),
                "captionStatus": video_data["contentDetails"]["caption"],
                "duration":video_data["contentDetails"]["duration"]
            }
           
        
    return video_info

def get_video_comments(api_key, video_id):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100
        ).execute()
        
        comments = []
        for item in response['items']:
            comment_info = {
                '_id': item['snippet']['topLevelComment']['id'],
                'channelId': item['snippet']['topLevelComment']['snippet']['channelId'],
                'videoId': video_id,
                'commentId': item['snippet']['topLevelComment']['id'],
                'commentText': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'commentAuthorName': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'commentPublishedDate': item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment_info)
        
        return comments
    except HttpError as e:
        if e.resp.status == 403:
            print(f"Comments are disabled for the video with ID {video_id}. Skipping fetching comments.")
            return []
        else:
            raise e


def main():
    # Load configuration from config file
    print("Read Config File...")
    config = load_config('config.json')

    # Fetch and store channel information
    for channel_id in config['channel_ids']:
        print("Get Channel Info..."  + channel_id)
        channel_info = get_channel_info(config['api_key'], channel_id)
        print("Save Channel Info..." )
        save_channel_info_to_mongodb(config, channel_info)

        # Fetch and store video information
        video_ids = get_video_ids(config['api_key'], channel_id)
        
        for video in video_ids:
            video_inform = get_videoinfo_from_video(config['api_key'], video)
            
            save_video_info_to_mongodb(config, video_inform)

            # Fetch and store comments for each video            
            comments = get_video_comments(config['api_key'], video)
            save_comments_to_mongodb(config, comments)

if __name__ == "__main__":
    main()
