import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine,inspect
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError



def connect_to_mongo_db_cluster():
    cluster = MongoClient('mongodb+srv://abirajan12:nNpLGsIHpUpx1Yrp@ytcluster.aajb4yv.mongodb.net/?retryWrites=true&w=majority')
    db = cluster['YtHarv']
    col = db['YtColl']
    return col

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
    
def get_channel_Id_List():
    col = connect_to_mongo_db_cluster()

    channel_Ids_List = []
    list_of_channel_Ids = list(col.find({},{"_id":0,"channel_Data.channel_Id":1}) )

    for i in range(len(list_of_channel_Ids)):
        channel_Ids_List.append(list_of_channel_Ids[i]['channel_Data']['channel_Id'])
    return channel_Ids_List

def channel_list():
    col = connect_to_mongo_db_cluster()

    list_of_channels = []
    list_of_channel_names = list(col.find({},{"_id":0,"channel_Data.channel_Name":1}) )
    
    for i in range(len(list_of_channel_names)):
        list_of_channels.append(list_of_channel_names[i]['channel_Data']['channel_Name'])
    return list_of_channels

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
    
def transform_channel_df(channel_df):
    channel_df['channel_Views'] = channel_df['channel_Views'].astype(np.int64)
    channel_df['subscription_Count']= channel_df['subscription_Count'].astype(np.int64)
    return channel_df

def get_channel_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    channel_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"channel_Data":1}))
    channel_Items = channel_Items[0]['channel_Data']
    channel_df = pd.DataFrame([channel_Items])
    transform_channel_df(channel_df)
    return channel_df

def transform_playlist_df(playlist_df):
    pass

def get_playlist_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    playlist_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"playlist_Data":1}))
    
    for i in range(len(playlist_Items)):
        playlist_Items = playlist_Items[i]['playlist_Data']
        
    playlist_df = pd.DataFrame(playlist_Items)
    transform_playlist_df(playlist_df)
    return playlist_df

def change_duration_datatype(video_df):
    video_df[['minutes', 'seconds']] = video_df['duration'].str.extract(r'PT(?:(\d+)M)?(\d+)S').fillna(0).astype(int)
    video_df['duration'] = video_df['minutes'] * 60 + video_df['seconds']
    video_df.drop(['minutes', 'seconds'], axis=1, inplace = True)
    return video_df

def transform_video_df(video_df):
    video_df['tags'] = video_df['tags'].astype(str)
    video_df['published_at'] = pd.to_datetime(video_df['published_at'], format="%Y-%m-%dT%H:%M:%SZ")
    video_df['view_count'] = video_df['view_count'].astype(np.int64)
    video_df['like_count'] = video_df['like_count'].astype(np.int64)
    video_df['dislike_count'] = video_df['dislike_count'].astype(np.int64)
    video_df['favorite_count'] = video_df['favorite_count'].astype(int)
    video_df['comment_count'] = video_df['comment_count'].astype(np.int64)

    video_df = change_duration_datatype(video_df)
    return video_df

def get_video_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    video_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"video_Data":1}))

    for i in range(len(video_Items)):
        video_Items = video_Items[i]['video_Data']

    video_df = pd.DataFrame(video_Items)
    transform_video_df(video_df)
    return video_df

def transform_comment_df(comment_df):
    comment_df['comment_Like_Count'] = comment_df['comment_Like_Count'].astype(int)
    comment_df['comment_Published_At'] = pd.to_datetime(comment_df['comment_Published_At'], format="%Y-%m-%dT%H:%M:%SZ")
    return comment_df

def get_comment_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    comment_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"comment_Data":1}))
    
    for i in range(len(comment_Items)):
        comment_Items = comment_Items[i]['comment_Data']

    if comment_Items:
    #l = pd.DataFrame(comment_Items)
    #l = l.dropna()    
        comment_df = pd.DataFrame(comment_Items).iloc[:,:-1]
    #reply_df = pd.DataFrame(comment_Items).iloc[:,-1]
    #print(pd.DataFrame(comment_Items).iloc[:,:-1].head(4))
    #print(reply_df.tail(4))
    #print(l['comment_replies'])
    #comment_df = pd.DataFrame({'comment_Id':l['comment_Id'],
      #                         'comment_Text':l['comment_Text'],
     #                          'comment_Author':l['comment_Author']})
    #k = pd.DataFrame(l['comment_replies'])
    #k.dropna()
    #print(k)
    #print(comment_df.head(4))
    #print(l.head(4))
    #reply_df = pd.DataFrame({'comment_Id':k[0]['comment_Id'],
     #                          'comment_Text':k[0]['comment_Text'],
      #                         'comment_Author':k[0]['comment_Author']})
    #all_comment_df = pd.concat([comment_df,reply_df],ignore_index=True)
        transform_comment_df(comment_df)
        return comment_df
    else:
        return
    
def insert_data_json_to_mongodb_cluster(data):
    col = connect_to_mongo_db_cluster()

    channel_Id = data['channel_Data']['channel_Id']

    # Check if a document with the same channel_Id already exists
    existing_document = col.find_one({'channel_Data.channel_Id': channel_Id})
    if existing_document:
        st.sidebar.write(f"Document with channel_Id {channel_Id} already exists in Mongodb. Skipping insertion.")
        return

    # Insert the document into the collection
    col.insert_one(data)
    st.sidebar.write(f"Document with channel_Id {channel_Id} inserted successfully.")

def get_channel_Data(youtube, channel_Id):
    request = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                      id = channel_Id)
    response = request.execute()
    
    channel_Data = {"channel_Id":channel_Id,
                    "channel_Name":response['items'][0]['snippet']['title'],
                    "channel_Views":response['items'][0]['statistics']['viewCount'],
                    "channel_Description":response['items'][0]['snippet']['description'],
                    "subscription_Count":response['items'][0]['statistics']['subscriberCount'],
                    "playlist_Id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_Data

def get_comment_Data(youtube, video_Ids, max_results=100):
    comments_Data = []
    for video_Id in video_Ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_Id,
                maxResults=max_results
            )
            while request:
                response = request.execute()
                for item in response['items']:
                    # Extract comment data
                    comment_snippet = item['snippet']['topLevelComment']['snippet']
                    comment = {
                        'comment_Id': item['id'],
                        'video_Id': comment_snippet['videoId'],
                        'comment_Text': comment_snippet['textDisplay'],
                        'comment_Author': comment_snippet['authorDisplayName'],
                        'comment_Like_Count': comment_snippet['likeCount'],
                        'comment_Published_At': comment_snippet['publishedAt'],
                    }

                    # Extract comment replies
                    if 'replies' in item:
                        for reply_item in item['replies']['comments']:
                            reply = {
                                'comment_Id': reply_item['id'],
                                'video_Id': reply_item['snippet']['videoId'],
                                'comment_Text': reply_item['snippet']['textDisplay'],
                                'comment_Author': reply_item['snippet']['authorDisplayName'],
                                'comment_Like_Count': reply_item['snippet']['likeCount'],
                                'comment_Published_At': reply_item['snippet']['publishedAt']
                            }
                            comment.update({'comment_replies': reply})
                    comments_Data.append(comment)

                # Check if there are more comments
                if 'nextPageToken' in response:
                    next_page_token = response['nextPageToken']
                    request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_Id,
                        maxResults=max_results,
                        pageToken=next_page_token
                    )
                else:
                    break
        except:
            print("Comments may be disabled for video ID:", video_Id)
            continue
    return comments_Data

# def get_comment_Data(youtube,video_Ids):

#     comments_Data=[]
#     for video_Id in video_Ids:    
#         try:
#             request = youtube.commentThreads().list(
#                 part="snippet,replies",
#                 videoId = video_Id
#                 )
#             response = request.execute()
#             for item in response['items']:
#                 # Extract comment data
#                 comment_snippet = item['snippet']['topLevelComment']['snippet']
#                 comment = {
#                     'comment_Id': item['id'],
#                     'video_Id': comment_snippet['videoId'],
#                     'comment_Text': comment_snippet['textDisplay'],
#                     'comment_Author': comment_snippet['authorDisplayName'],
#                     'comment_Like_Count': comment_snippet['likeCount'],
#                     'comment_Published_At': comment_snippet['publishedAt'],
#                 }

#                 # Extract comment replies
#                 if 'replies' in item:
#                     for reply_item in item['replies']['comments']:
#                         reply = {
#                             'comment_Id': reply_item['id'],
#                             'video_Id'  : reply_item['snippet']['videoId'],
#                             'comment_Text': reply_item['snippet']['textDisplay'],
#                             'comment_Author': reply_item['snippet']['authorDisplayName'],
#                             'comment_Like_Count': reply_item['snippet']['likeCount'],
#                             'comment_Published_At': reply_item['snippet']['publishedAt']
#                         }
#                         comment.update({'comment_replies':reply})
#                 comments_Data.append(comment)
#         except:
#             print("comments may be disabled")
#             continue
#     return comments_Data

def get_video_Data(youtube, video_Ids):
    video_Data = []
    page_token = None
    max_results = 50  # Maximum number of video IDs per request

    # Split the video IDs into chunks of maximum size
    video_ids_chunks = [video_Ids[i:i+max_results] for i in range(0, len(video_Ids), max_results)]

    for chunk in video_ids_chunks:
        # Convert video IDs chunk to a comma-separated string
        video_ids_str = ','.join(chunk)

        while True:
            # Get video details with the specified page token
            request = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_ids_str,
                pageToken=page_token
            )
            response = request.execute()

            for item in response['items']:
                # Extract relevant information from the API response
                video_info = {
                    'video_id': item['id'],
                    'channel_title': item['snippet']['channelTitle'],
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'tags': ",".join(item['snippet'].get('tags', '')),
                    'published_at': item['snippet']['publishedAt'],
                    'view_count': item['statistics'].get('viewCount', 0),
                    'like_count': item['statistics'].get('likeCount', 0),
                    'dislike_count': item['statistics'].get('dislikeCount', 0),
                    'favorite_count': item['statistics'].get('favoriteCount', 0),
                    'comment_count': item['statistics'].get('commentCount', 0),
                    'duration': item['contentDetails']['duration'],
                    'definition': item['contentDetails'].get('definition', ''),
                    'caption': item['contentDetails'].get('caption', False)
                }

                video_Data.append(video_info)

            # Check if there are more pages to retrieve
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']
            else:
                break

    return video_Data

def get_video_IDs(youtube, playlist_Id):
    video_Ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(part="contentDetails",
                                               playlistId = playlist_Id, 
                                               pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            video_Ids.append(item['contentDetails']['videoId'])
            
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return video_Ids

def get_playlist_Data(playlist_Id, video_Ids):

    playlist_Data = []    
    for i in range(len(video_Ids)):
            playlist_Data.append({'playlist_Id': playlist_Id, 'video_Id': video_Ids[i]})
   
    return playlist_Data

def get_Youtube_Data(api_Key, channel_Id):
    youtube = build('youtube','v3',developerKey = api_Key)

    channel_Data  = get_channel_Data(youtube, channel_Id)
    video_Ids     = get_video_IDs(youtube, channel_Data['playlist_Id'])
    playlist_Data = get_playlist_Data(channel_Data['playlist_Id'], video_Ids)
    video_Data    = get_video_Data(youtube,video_Ids)
    comment_Data  = get_comment_Data(youtube,video_Ids)

        
    data = {'channel_Data':channel_Data,
            'playlist_Data':playlist_Data,
            'video_Data':video_Data,
            'comment_Data':comment_Data
            } 

    return data  


def get_YT_Data():
    # Adding a text box for API Key
    api_Key = st.sidebar.text_input("Enter the API Key:")
    
    # Adding a button to get API Key
    button_Clicked = st.sidebar.button("submit")
    
    # Retrieving all the details related to the channel ID
    if button_Clicked:
            st.write("API Key is "+api_Key)

    # Getting the channel id
    channel_Id = st.sidebar.text_input("Enter the Channel ID:")
    get_Data = st.sidebar.button("Get Data")

    if channel_Id and get_Data:
        data = get_Youtube_Data(api_Key, channel_Id)

        # Displaying the data in UI
        st.write("Expand to view Channel Details")
        data_json = st.json(data,expanded=False)
        
        if channel_Id in get_channel_Id_List():
            st.sidebar.write(f"{channel_Id} Data already present in MongoDB")
        else:
            #Inserting the data in json format to a collection mongodb ATLAS
            insert_data_json_to_mongodb_cluster(data)

def migrate_Data():
    list_channel_df = []
    list_playlist_df = []
    list_video_df = []
    list_comment_df = []

    mig_Data = st.sidebar.multiselect("Select the channel",channel_list())
    
    for i in range(len(mig_Data)):
        st.title("Channel DataFrame")
        channel_df = get_channel_df(mig_Data[i])
        list_channel_df.append(channel_df)
        st.dataframe(channel_df)

        st.title("Playlist Dataframe")
        playlist_df = get_playlist_df(mig_Data[i])
        list_playlist_df.append(playlist_df)
        st.dataframe(playlist_df)

        st.title("Videos Dataframe")
        video_df = get_video_df(mig_Data[i])
        list_video_df.append(video_df)
        st.dataframe(video_df)

        st.title("Comment Dataframe")
        comment_df = get_comment_df(mig_Data[i])
        list_comment_df.append(comment_df)
        st.dataframe(comment_df)

        #st.sidebar.write("Credentials for Mysql")

        #username = st.sidebar.text_input("Username")
        #password = st.sidebar.text_input("Password")

    mig_Data_button = st.sidebar.button("Migrate Data")

    if mig_Data_button:

        for i in range(len(mig_Data)): 
            msg = migrate_to_mysql(mig_Data[i],list_channel_df[i],list_playlist_df[i],list_video_df[i],list_comment_df[i])
            st.sidebar.write(msg)

def query_Data():
    query_List = ['Drag to choose query','What are the names of all the videos and their corresponding channels?',
                  'Which channels have the most number of videos, and how many videos do they have?',
                   'What are the top 10 most viewed videos and their respective channels?',
                   'How many comments were made on each video, and what are their corresponding video names?',
                   'Which videos have the highest number of likes, and what are their corresponding channel names?',
                   'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                   'What is the total number of views for each channel, and what are their corresponding channel names?',
                   'What are the names of all the channels that have published videos in the year 2022?',
                   'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                   'Which videos have the highest number of comments, and what are their corresponding channel names?' ]
    query = st.selectbox('select the Query',query_List)
    engine = create_engine(f'mysql+pymysql://root:Guviabi12*@localhost/YTProject')
    if query == 'Drag to choose query':
        pass    
    elif query == 'What are the names of all the videos and their corresponding channels?':
        mysql_Query = "SELECT title as VIDEO_NAME, channel_title as CHANNEL_NAME\
                        FROM video"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'Which channels have the most number of videos, and how many videos do they have?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, COUNT(*) AS VIDEO_COUNT \
                        FROM video \
                        GROUP BY CHANNEL_NAME \
                        ORDER BY VIDEO_COUNT DESC lIMIT 1"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'What are the top 10 most viewed videos and their respective channels?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, title as VIDEO_NAME \
                        FROM video \
                        ORDER BY view_count DESC LIMIT 10;"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'How many comments were made on each video, and what are their corresponding video names?':
        mysql_Query = "SELECT count(comment_Id) as COMMENT_COUNT, title as VIDEO_NAME\
                        FROM comment, video \
                        WHERE comment.video_Id = video.video_id \
                        GROUP BY VIDEO_NAME"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
        mysql_Query = "SELECT video_id AS VIDEO_ID, title AS VIDEO_NAME, like_count AS LIKE_COUNT, channel_title as CHANNEL_NAME \
                        FROM video \
                        ORDER BY like_count DESC LIMIT 1"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mysql_Query = "SELECT title as VIDEO_NAME, like_count as LIKE_COUNT, dislike_count as DISLIKE_COUNT \
                        FROM VIDEO"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'What is the total number of views for each channel, and what are their corresponding channel names?':
        mysql_Query = "SELECT channel_Name as CHANNEL_NAME, channel_Views as TOTAL_VIEWS \
                        FROM channel"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'What are the names of all the channels that have published videos in the year 2022?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, published_at as PUBLISHED_AT \
                        FROM video \
                        WHERE YEAR(published_at) = 2022"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mysql_Query = "SELECT channel_title as CHANNEL_NAME, avg(duration) as AVG_DURATION \
                        FROM video \
                        GROUP BY CHANNEL_NAME"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    elif query == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
        mysql_Query = "SELECT count(comment_Id) AS COMMENT_COUNT, channel_title AS CHANNEL_NAME \
                        FROM comment c\
                        JOIN video v\
                        ON c.video_Id = v.video_id\
                        GROUP BY CHANNEL_NAME\
                        ORDER BY COMMENT_COUNT DESC LIMIT 1;"
        df = pd.read_sql(mysql_Query, engine)
        st.dataframe(df)
    


def main():
    
    yt_task = st.sidebar.selectbox("What would you like to do?",
                                        ('Get Data', 'Migrate Data','Query Data'))
    
    if yt_task == 'Get Data':
         # Page title
        st.title("Youtube Data Harvesting and Warehousing")
        
        get_YT_Data()

    elif yt_task == 'Migrate Data':
        migrate_Data()
    elif yt_task == 'Query Data':
        query_Data()

if __name__ == "__main__":
    main()
