import json
import mysql.connector
import pymongo
import pymysql
import datetime
from pymongo import MongoClient


def load_config(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def connect_to_mongodb(config):
    client = MongoClient(config['mongo']['connection_string'])
    db = client[config['mongo']['db_name']]
    return db

def connect_to_mysql(config):
    return mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    
# Function to convert datetime string from MongoDB to MySQL format
def convert_datetime(datetime_str):
    # Parse the datetime string from MongoDB
    datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
    # Convert the datetime object to MySQL format string
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

def convert_duration(duration):
    import re
    duration_regex = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = duration_regex.match(duration)
    if match:
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        total_seconds = hours * 3600 + minutes * 60 + seconds
        modified_duration='{:02d}:{:02d}:{:02d}'.format(total_seconds // 3600, (total_seconds % 3600) // 60, total_seconds % 60)
        return modified_duration
    return '00:00:00'


def create_mysql_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channelId VARCHAR(50) PRIMARY KEY,
            title VARCHAR(255),
            playlistId VARCHAR(255),
            channelType TEXT,
            description TEXT,
            videoCount INT,
            subscriberCount INT
        )
    """)

    cursor.execute("""  
        CREATE TABLE IF NOT EXISTS videos (
            videoId VARCHAR(50) PRIMARY KEY,
            channelId VARCHAR(255),
            videoName VARCHAR(255),
            videoDescription TEXT,
            thumbnailUrl VARCHAR(255),
            publishedAt DATETIME,
            viewCount INT,
            likeCount INT,
            dislikeCount INT,
            commentCount INT,
            favoriteCount INT,
            captionStatus TEXT,
            duration TIME,
            FOREIGN KEY (channelId) REFERENCES channels(channelId)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            commentId VARCHAR(50) PRIMARY KEY,
            videoId VARCHAR(50),
            channelId VARCHAR(50),
            commentText TEXT,
            commentAuthorName VARCHAR(255),
            commentPublishedDate DATETIME,
            FOREIGN KEY (videoId) REFERENCES videos(videoId),
            FOREIGN KEY (channelId) REFERENCES channels(channelId)
        )
    """)

def migrate_data_to_mysql(config, db, mysql_conn):
    cursor = mysql_conn.cursor()

    # Create MySQL tables
    create_mysql_tables(cursor)

    # Migrate channel data
    channels_collection = db[config['mongo']['channel_collection']]
    for channel in channels_collection.find():
        cursor.execute("""SELECT * FROM channels WHERE channelId = %s """, (channel['channelId'],))
        existing_channel = cursor.fetchone()
        if existing_channel:
            cursor.execute("""
                UPDATE channels
                SET title = %s
                ,playlistId=%s
                ,channelType=%s
                , description = %s
                , videoCount = %s
                , subscriberCount = %s
                WHERE channelId = %s
            """, (channel['title']                  
                  , channel['playlistId']
                  ,channel['channelType'] 
                  ,channel['description']
                  , channel['videoCount']
                  , channel['subscriberCount']
                  ,channel['channelId']))
        else:
            cursor.execute("""
                INSERT INTO channels (channelId
                ,title
                ,playlistId
                ,channelType
                ,description
                ,videoCount
                ,subscriberCount)
                VALUES (%s, %s, %s, %s, %s,%s,%s)
            """, (channel['channelId']
                  , channel['title']
                  , channel['playlistId']
                  ,channel['channelType']
                  ,channel['description']
                  , channel['videoCount']
                  , channel['subscriberCount']))
    
    mysql_conn.commit()    
    # Migrate video data
    videos_collection = db[config['mongo']['video_collection']]
    for video in videos_collection.find():
        cursor.execute("""
            SELECT * FROM videos WHERE videoId = %s
        """, (video['videoId'],))
        existing_video = cursor.fetchone()
        if existing_video:
            cursor.execute("""
                UPDATE videos
                SET channelId = %s, videoName = %s, videoDescription = %s, thumbnailUrl = %s, publishedAt = %s,
                viewCount=%s,likeCount=%s,dislikeCount=%s,commentCount=%s,favoriteCount=%s,captionStatus=%s,duration=%s
                WHERE videoId = %s
            """, (video['channelId']
                  , video['videoName']
                  , video['videoDescription']
                  , video['thumbnailUrl']
                  , convert_datetime(video['publishedAt'])
                  , video['viewCount']
                  , video['likeCount']
                  , video['dislikeCount']
                  , video['commentCount']
                  , video['favoriteCount']
                  , video['captionStatus']
                  , convert_duration(video['duration'])
                  ,video['videoId']
                   ))
        else:
            cursor.execute("""
                INSERT INTO videos (videoId
                , channelId
                , videoName
                , videoDescription
                , thumbnailUrl
                , publishedAt
                , viewCount
                , likeCount
                , dislikeCount
                , commentCount
                , favoriteCount
                , captionStatus
                , duration)
                VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)
            """, (video['videoId']
                  , video['channelId']
                  , video['videoName']
                  , video['videoDescription']
                  , video['thumbnailUrl']
                  , convert_datetime(video['publishedAt'])
                  , video['viewCount']
                  , video['likeCount']
                  , video['dislikeCount']
                  , video['commentCount']
                  , video['favoriteCount']
                  , video['captionStatus']
                  , convert_duration(video['duration'])))

        
    # Migrate comment data
    comments_collection = db[config['mongo']['comment_collection']]
    for comment in comments_collection.find():
        cursor.execute("""
            SELECT * FROM comments WHERE commentId = %s
        """, (comment['commentId'],))
        existing_comment = cursor.fetchone()
        if existing_comment:
            cursor.execute("""
                UPDATE comments
                SET videoId = %s, channelId = %s, commentText = %s, commentAuthorName = %s, commentPublishedDate = %s
                WHERE commentId = %s
            """, (comment['videoId']
                  , comment['channelId']
                  , comment['commentText']
                  , comment['commentAuthorName']
                  , convert_datetime(comment['commentPublishedDate'])
                  , comment['commentId']))
        else:
            cursor.execute("""
                INSERT INTO comments (commentId, videoId, channelId, commentText, commentAuthorName, commentPublishedDate)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (comment['commentId']
                  , comment['videoId']
                  , comment['channelId']
                  , comment['commentText']
                  , comment['commentAuthorName']
                  , convert_datetime(comment['commentPublishedDate'])))

    mysql_conn.commit()
    cursor.close()


def main():
    # Load configuration from config file
    config = load_config('config.json')

    # Connect to MongoDB
    db = connect_to_mongodb(config)

    # Connect to MySQL
    mysql_conn = connect_to_mysql(config)

    # Migrate data from MongoDB to MySQL
    migrate_data_to_mysql(config, db, mysql_conn)

    # Close MySQL connection
    mysql_conn.close()

if __name__ == "__main__":
    main()
