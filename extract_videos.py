import json
import requests
import os
import boto3
from datetime import datetime

def get_all_videos_in_channel(channel_id, api_url, headers):
    all_videos = []
    next_page_token = None
    channel_name = "Unknown_Channel"

    while True:
        querystring = {
            "channelId": channel_id,
            "part": "snippet,id",
            "order": "date",
            "maxResults": "50",
            "type": "video"
        }
        
        if next_page_token:
            querystring["pageToken"] = next_page_token
            
        response = requests.get(api_url, headers=headers, params=querystring)
        data = response.json()  # Store the response in data
        required_data = data.get("items", [])
        for video in required_data:
            formatted_data = {
                        "channel_id" : video['snippet']['channelId'],
                        "channel_name" : video['snippet']['channelTitle'],
                        "video_id" : video['id']['videoId'],
                        "video_title" : video['snippet']['title'],
                        "published_at" : video['snippet']['publishedAt'],
                        "description" : video['snippet']['description'],
                        "thumbnails" : video['snippet']['thumbnails']
                    }
            
            all_videos.append(formatted_data)  # Extract items safely

        next_page_token = data.get("nextPageToken")

        if not next_page_token:
            channel_name = all_videos[0]['channel_name'] if all_videos else "Unknown_Channel"
            break
        
    return all_videos, channel_name

def lambda_handler(event, context):
    channel_ids = ["UCw9ygW4Y6ZfFgqYQmGbUlAQ","UCjyj4NMlpiDBLkW2MtgbzJw"]

    url = "https://youtube-v31.p.rapidapi.com/search"

    api_key = os.getenv('x_rapidapi_key')
    if not api_key:
        raise ValueError("API key not found. Set 'x_rapidapi_key' as an environment variable.")

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "youtube-v31.p.rapidapi.com"
    }
    for channel_id in channel_ids:
        all_videos, channel_name = get_all_videos_in_channel(channel_id, url, headers)

        client = boto3.client('s3')
        bucket_name = 'nazira-youtube-etl-pipeline'

        # Check if the bucket exists before creating
        existing_buckets = [b['Name'] for b in client.list_buckets()['Buckets']]
        if bucket_name not in existing_buckets:
            client.create_bucket(Bucket=bucket_name)

        file_name = f'{channel_name}_{channel_id}.json'
        s3_key = f'extracted_data/videos/raw/{file_name}'

        client.put_object(
            Body=json.dumps(all_videos),
            Bucket=bucket_name, 
            Key=s3_key
        )

        print(f"Total Videos Fetched: {len(all_videos)}")