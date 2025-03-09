import json
import boto3
import pandas as pd
from io import StringIO

def get_videos_df(client, bucket_name):
    response = client.list_objects_v2(Bucket=bucket_name, Prefix='extracted_videos/processed/')

    # Extract file names from the response
    files_to_transform = [obj['Key'] for obj in response.get('Contents', [])]

    for idx,file in enumerate(files_to_transform):
        data = client.get_object(Bucket=bucket_name, Key=file)
        json_data = data['Body'].read()
        data = json.loads(json_data)
            
        videos_per_channel_df = pd.DataFrame(data)
        if idx == 0:
            videos_df = videos_per_channel_df
        else:
            videos_df = pd.concat([videos_df, videos_per_channel_df])
    videos_df = videos_df.reset_index(drop = True)

    videos_df['thumbnails'] = videos_df['thumbnails'].apply(lambda x: x['default']['url'])
    videos_df['video_title'] = videos_df['video_title'].str.split("|").apply(lambda x: x[0])
    videos_df['published_at'] = pd.to_datetime(videos_df['published_at'])
    return videos_df

def get_comments_df(client, bucket_name):
    response = client.list_objects_v2(Bucket=bucket_name, Prefix='extracted_comments/')

    # Extract file names from the response
    files_to_transform = [obj['Key'] for obj in response.get('Contents', [])]

    for idx,file in enumerate(files_to_transform):
        data = client.get_object(Bucket=bucket_name, Key=file)
        json_data = data['Body'].read()
        data = json.loads(json_data)
        for idx1, (video_id, video_comments) in enumerate(data.items()):
            comment_per_video_df = pd.DataFrame(video_comments)
            comment_per_video_df['video_id'] = video_id
            if idx1 == 0:
                comments_per_video_per_channel_df = comment_per_video_df
            else:
                comments_per_video_per_channel_df = pd.concat([comments_per_video_per_channel_df, comment_per_video_df])
        if idx == 0:
            comments_df = comments_per_video_per_channel_df
        else:
            comments_df = pd.concat([comments_df, comments_per_video_per_channel_df])

    comments_df['author_channel_id'] = comments_df['author_channel_id'].apply(lambda x: x['value'])
    comments_df['publishedAt'] = pd.to_datetime(comments_df['publishedAt'])
    comments_df['updatedAt'] = pd.to_datetime(comments_df['updatedAt'])

    comments_df = comments_df.reset_index(drop = True)
    return comments_df

def lambda_handler(event, context):
    
    bucket_name = 'nazira-youtube-etl-pipeline'
    client = boto3.client('s3')
    videos_df = get_videos_df(client, bucket_name)
    comments_df = get_comments_df(client, bucket_name)

    # Store transformed videos_df to s3
    csv_buffer = StringIO()
    videos_df.to_csv(csv_buffer, index=False) 
    client.put_object(Bucket = bucket_name,
                        Body = csv_buffer.getvalue(),
                        Key = 'transformed_data/videos/videos.csv')
    
    # Store transformed comments_df to s3
    csv_buffer = StringIO()
    comments_df.to_csv(csv_buffer, index=False) 
    client.put_object(Bucket = bucket_name,
                        Body = csv_buffer.getvalue(),
                        Key = 'transformed_data/comments/comments.csv')
