import os
import logging
import googleapiclient.discovery
import numpy as np
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

load_dotenv()

DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY")
if not DEVELOPER_KEY:
    raise ValueError("DEVELOPER_KEY is not set in the .env file")

api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=DEVELOPER_KEY)

POSTGRES_CONN_ID='postgres_default'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

with DAG(dag_id='Youtube_data_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:

    @task()
    def extract_youtube_data():
        """Extract data from Youtube api"""
        
        logging.info("Sending request to YouTube API to fetch video data.")
        
        request = youtube.search().list(
                part="snippet",
                maxResults=100,
                type="video"
        )
        
        response = request.execute()
        
        logging.info("Response received from YouTube API.")
    
        if 'items' not in response:
            logging.error("No 'items' found in response.")
            raise ValueError("Failed to extract data from YouTube API.")

        return response

    @task()
    def transform_youtube_data(youtube_data):
        """Transformed data"""
        video_infos = youtube_data.get('items', [])
        logging.info(f"Transforming {len(video_infos)} videos.")
        
        if not video_infos:
            logging.warning("No video data found in YouTube API response.")
            return []
        
        # Process each video
        videos = []
        for video in video_infos:
            try:
                video_id = video['id']['videoId']
                logging.info(f"Processing video ID: {video_id}")
                
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

                video = {'Id': video_id,
                        'Title': title,
                        'Channel': channel_name,
                        'Language': language,
                        'Views': views,
                        'Likes': likes,
                        'Comment count': comment_count
                        }

                videos.append(video)
                
            except Exception as e:
                logging.error(f"Failed to process video ID: {video_id}. Error: {e}")
                continue
            
        return videos

    @task()
    def load_youtube_data(videos):
        """Load data into PostgresSQL"""
        logging.info(f"Starting to load {len(videos)} videos into PostgreSQL.")
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create table if not exists
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
        
        for video in videos:
            try:
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
            
            except Exception as e:
                logging.error(f"Failed to insert video ID: {video['Id']} into PostgreSQL. Error: {e}")
                continue

        conn.commit()
        conn.close()
        logging.info("Data loading into PostgreSQL completed successfully.")
        
    ## DAG Worflow- ETL Pipeline
    youtube_data= extract_youtube_data()
    transformed_data = transform_youtube_data(youtube_data)
    load_youtube_data(transformed_data)
