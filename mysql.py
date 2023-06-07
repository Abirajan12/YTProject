from sqlalchemy import create_engine,inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

def migrate_to_mysql(channel_name,channel_df,playlist_df,video_df,comment_df):
    engine = create_engine(f'mysql+pymysql://root:Guviabi12*@localhost/YTProject')
    Session = sessionmaker(bind=engine)
    session = Session()

    if channel_name in get_migrated_channel_names():
        msg = f"{channel_name} channel Data already exists"
        return msg
    else:
        try:
            with session.begin():
                channel_df.to_sql('channel', con=engine, if_exists='append', index=False)
                playlist_df.to_sql('playlist', con=engine, if_exists='append', index=False)
                video_df.to_sql('video', con=engine, if_exists='append', index=False)
                comment_df.to_sql('comment', con=engine, if_exists='append', index=False)
        except SQLAlchemyError as e:
            # Handle the exception if any operation fails
            session.rollback()
            print("An error occurred:", str(e))  
        finally:
            # Close the session
            session.close()
            msg = f"{channel_name} channel Data got inserted to mysql"
            return msg 
    

def get_migrated_channel_names():
    engine = create_engine(f'mysql+pymysql://root:Guviabi12*@localhost/YTProject')
    # Create an inspector object
    inspector = inspect(engine)

    # Check if the table "channel" exists
    if inspector.has_table("channel"):
        query = "SELECT channel_Name FROM channel"
        migrated_df = pd.read_sql(query, engine)
        migrated_channel_names = migrated_df['channel_Name'].tolist()
        return migrated_channel_names
    else:
        return []

def get_Query_Result(query):
    engine = create_engine(f'mysql+pymysql://root:Guviabi12*@localhost/YTProject')
    if query == 'Drag to choose query':
        None    
    elif query == 'What are the names of all the videos and their corresponding channels?':
        mysql_Query = "SELECT title as VIDEO_NAME, channel_title as CHANNEL_NAME\
                        FROM video"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'Which channels have the most number of videos, and how many videos do they have?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, COUNT(*) AS VIDEO_COUNT \
                        FROM video \
                        GROUP BY CHANNEL_NAME \
                        ORDER BY VIDEO_COUNT DESC lIMIT 1"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'What are the top 10 most viewed videos and their respective channels?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, title as VIDEO_NAME \
                        FROM video \
                        ORDER BY view_count DESC LIMIT 10;"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'How many comments were made on each video, and what are their corresponding video names?':
        mysql_Query = "SELECT count(comment_Id) as COMMENT_COUNT, title as VIDEO_NAME\
                        FROM comment, video \
                        WHERE comment.video_Id = video.video_id \
                        GROUP BY VIDEO_NAME"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
        mysql_Query = "SELECT video_id AS VIDEO_ID, title AS VIDEO_NAME, like_count AS LIKE_COUNT, channel_title as CHANNEL_NAME \
                        FROM video \
                        ORDER BY like_count DESC LIMIT 1"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mysql_Query = "SELECT title as VIDEO_NAME, like_count as LIKE_COUNT, dislike_count as DISLIKE_COUNT\
                        FROM VIDEO"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'What is the total number of views for each channel, and what are their corresponding channel names?':
        mysql_Query = "SELECT channel_Name as CHANNEL_NAME, channel_Views as TOTAL_VIEWS \
                        FROM channel"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'What are the names of all the channels that have published videos in the year 2022?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, published_at as PUBLISHED_AT \
                        FROM video \
                        WHERE YEAR(published_at) = 2022"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, avg(duration) as AVG_DURATION \
                        FROM video \
                        GROUP BY CHANNEL_NAME"
        df = pd.read_sql(mysql_Query, engine)
        return df
    elif query == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
        mysql_Query = "SELECT count(comment_Id) AS COMMENT_COUNT, channel_title AS CHANNEL_NAME \
                        FROM comment c\
                        JOIN video v\
                        ON c.video_Id = v.video_id\
                        GROUP BY CHANNEL_NAME\
                        ORDER BY COMMENT_COUNT DESC LIMIT 1;"
        df = pd.read_sql(mysql_Query, engine)
        return df