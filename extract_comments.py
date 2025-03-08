import json
import requests
import os
import boto3
from datetime import datetime

def get_comments_for_a_video(video, api_url, api_headers,comment_count):
    
    video_id = video['video_id']
    all_comments = []
    next_page_token = None  # Start with no token
    
    while True:
        querystring = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": "100"
        }
        
        # Include nextPageToken if it exists
        if next_page_token:
            querystring["pageToken"] = next_page_token
    
        response = requests.get(api_url, headers=api_headers, params=querystring)
        data = response.json()
        
        # Extract relevant details from each comment
        for item in data.get("items", []):
            comment_data = item['snippet']['topLevelComment']['snippet']
            formatted_comment = {
                "author": comment_data["authorDisplayName"],
                "author_profile_image": comment_data["authorProfileImageUrl"],
                "author_channel_url": comment_data["authorChannelUrl"],
                "author_channel_id": comment_data["authorChannelId"],
                "comment": comment_data["textDisplay"],
                "likes": comment_data.get("likeCount", 0),
                "publishedAt": comment_data["publishedAt"],
                "updatedAt": comment_data["updatedAt"]
            }
            all_comments.append(formatted_comment)
            comment_count += 1
        # Check for nextPageToken
        next_page_token = data.get("nextPageToken")
    
        if not next_page_token:
            break
    return (all_comments, comment_count)

def get_comments_of_all_videos(all_videos, api_url, api_headers):
    complete_channel_comments = dict()
    comment_count = 0
    for video in all_videos:
        video_id = video['video_id']
        complete_channel_comments[video_id], comment_count = get_comments_for_a_video(video, api_url, api_headers, comment_count)
    
    channel_name = video['channel_name']
    channel_id = video['channel_id']
    return (complete_channel_comments, channel_name, channel_id, comment_count)

def list_json_files_s3(bucket_name, prefix=""):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".json")]
    return []
    
def lambda_handler(event, context):
    
    api_url = "https://youtube-v31.p.rapidapi.com/commentThreads"
    api_headers = {
        "x-rapidapi-key": os.getenv('x_rapidapi_key'),
        "x-rapidapi-host": "youtube-v31.p.rapidapi.com"
    }
    bucket_name = "nazira-youtube-etl-pipeline"
    prefix = "extracted_data/videos/raw/"
    jsons_to_process = list_json_files_s3(bucket_name, prefix)
    print(f"Total {len(jsons_to_process)} videos for comment extraction")
    print()

    # process raw jsons containing video details of a channel 
    for raw_filename in jsons_to_process:
        print(f"Fetching comments for {raw_filename.split('/')[-1]}")
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=f'{raw_filename}')
        content = response["Body"].read().decode("utf-8")  # Read and decode the file
        all_videos = json.loads(content)

        # Get comments of all the videos in the channel
        complete_channel_comments, channel_name, channel_id, comment_count = get_comments_of_all_videos(all_videos, api_url, api_headers)

        # move the proccessed files
        s3.put_object(Body=content,
                      Bucket=bucket_name, 
                      Key=f'extracted_data/videos/processed/{raw_filename.split("/")[-1]}')
        s3.delete_object(Bucket=bucket_name, Key=f'{raw_filename}')
        
        # saving comments to s3
        comments_save_path = f"extracted_data/comments/"
        s3.put_object(Body=json.dumps(complete_channel_comments),
                      Bucket=bucket_name,
                      Key=f'{comments_save_path}{channel_name}_{channel_id}.json')
        
        print(f"{comment_count} comments fetched for all {len(complete_channel_comments)} videos and saved to {comments_save_path}/{channel_name}_{channel_id}.json")
        print()