import os
import logging
import googleapiclient.discovery
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY")

if not DEVELOPER_KEY:
    raise ValueError("DEVELOPER_KEY is not set in the .env file")

api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=DEVELOPER_KEY)

conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres", 
    port="5432"
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Youtube_data (
        Id VARCHAR(255) PRIMARY KEY,
        Title VARCHAR(255), 
        Channel VARCHAR(255), 
        Language VARCHAR(255), 
        Views INT, 
        Likes INT, 
        Comment_count INT
    );
""")

def extract_youtube_data():
    request = youtube.search().list(
        part="snippet",
        maxResults=200,
        type="video"
    )
    
    response = request.execute()
    
    video_info = response['items']
    
    video_ids = []
    for video in video_info:
        video_id = video['id']['videoId']
        video_ids.append(video_id)
    
    return video_ids

def transform_youtube_data(video_ids):
    videos = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        )
        response = request.execute()

        video_info = response['items'][0]

        title = video_info['snippet']['title']
        channel_name = video_info['snippet']['channelTitle']
        language = video_info['snippet'].get('defaultLanguage', np.nan)
        views = video_info['statistics'].get('viewCount', 0)
        likes = video_info['statistics'].get('likeCount', 0)
        comment_count = video_info['statistics'].get('commentCount', 0)

        video = {
            'Id': video_id,
            'Title': title,
            'Channel': channel_name,
            'Language': language,
            'Views': views,
            'Likes': likes,
            'Comment count': comment_count
        }

        videos.append(video)
        
    return videos

def load_youtube_data(videos):
    for video in videos:
        cursor.execute("""
            INSERT INTO Youtube_data (Id, Title, Channel, Language, Views, Likes, Comment_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Id) DO NOTHING;
        """, (
            video['Id'], 
            video['Title'], 
            video['Channel'], 
            video['Language'], 
            video['Views'], 
            video['Likes'], 
            video['Comment count']
        ))

    conn.commit()
    
    df = pd.DataFrame(videos)
    df.to_csv('videos.csv', index=False)

def run_youtube_etl():
    # Extract
    logging.info('Extracting youtube data')
    video_ids = extract_youtube_data()
    logging.info(f'Extracted {len(video_ids)} video ids')
    
    # Transform
    logging.info('Transforming youtube data')
    videos = transform_youtube_data(video_ids)
    logging.info(f'Transformed {len(videos)} videos')
    
    # Load
    logging.info('Loading youtube data')
    load_youtube_data(videos)
    logging.info('Youtube data loaded into videos.csv')

if __name__ == "__main__":
    run_youtube_etl()
    conn.close()
