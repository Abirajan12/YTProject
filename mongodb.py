from pymongo import MongoClient
import pandas as pd
import numpy as np

def connect_to_mongo_db_cluster():
    cluster = MongoClient('mongodb+srv://abirajan12:nNpLGsIHpUpx1Yrp@ytcluster.aajb4yv.mongodb.net/?retryWrites=true&w=majority')
    db = cluster['YtHarv']
    col = db['YtColl']
    return col

def insert_data_json_to_mongodb_cluster(data):
    col = connect_to_mongo_db_cluster()

    channel_Id = data['channel_Data']['channel_Id']

    # Check if a document with the same channel_Id already exists
    existing_document = col.find_one({'channel_Data.channel_Id': channel_Id})
    if existing_document:
        msg = f"Document with channel_Id {channel_Id} already exists in Mongodb. Skipping insertion."
        return msg

    # Insert the document into the collection
    col.insert_one(data)
    msg = f"Document with channel_Id {channel_Id} inserted successfully."
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

def get_channel_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    channel_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"channel_Data":1}))
    channel_Items = channel_Items[0]['channel_Data']
    channel_df = pd.DataFrame([channel_Items])
    transform_channel_df(channel_df)
    return channel_df

def transform_channel_df(channel_df):
    channel_df['channel_Views'] = channel_df['channel_Views'].astype(np.int64)
    channel_df['subscription_Count']= channel_df['subscription_Count'].astype(np.int64)
    return channel_df

def get_playlist_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    playlist_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"playlist_Data":1}))
    
    for i in range(len(playlist_Items)):
        playlist_Items = playlist_Items[i]['playlist_Data']
        
    playlist_df = pd.DataFrame(playlist_Items)
    transform_playlist_df(playlist_df)
    return playlist_df

def transform_playlist_df(playlist_df):
    pass

def get_video_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    video_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"video_Data":1}))

    for i in range(len(video_Items)):
        video_Items = video_Items[i]['video_Data']

    video_df = pd.DataFrame(video_Items)
    transform_video_df(video_df)
    return video_df

def transform_video_df(video_df):
    video_df['tags'] = video_df['tags'].astype(str)
    video_df['published_at'] = pd.to_datetime(video_df['published_at'], format="%Y-%m-%dT%H:%M:%SZ")
    video_df['view_count'] = video_df['view_count'].astype(np.int64)
    video_df['like_count'] = video_df['like_count'].astype(np.int64)
    video_df['dislike_count'] = video_df['dislike_count'].astype(int)
    video_df['favorite_count'] = video_df['favorite_count'].astype(int)
    video_df['comment_count'] = video_df['comment_count'].astype(np.int64)

    video_df = change_duration_datatype(video_df)
    return video_df

def change_duration_datatype(video_df):
    video_df[['minutes', 'seconds']] = video_df['duration'].str.extract(r'PT(?:(\d+)M)?(\d+)S').fillna(0).astype(int)
    video_df['duration'] = video_df['minutes'] * 60 + video_df['seconds']
    video_df.drop(['minutes', 'seconds'], axis=1, inplace = True)
    return video_df

def get_comment_df(mig_Data):
    col = connect_to_mongo_db_cluster()
    comment_Items = list(col.find({"channel_Data.channel_Name":mig_Data},{"_id":0,"comment_Data":1}))
    
    for i in range(len(comment_Items)):
        comment_Items = comment_Items[i]['comment_Data']

    if comment_Items:  
        comment_df = pd.DataFrame(comment_Items).iloc[:,:-1]
        transform_comment_df(comment_df)
        return comment_df
    else:
        return
    
def transform_comment_df(comment_df):
    comment_df['comment_Like_Count'] = comment_df['comment_Like_Count'].astype(int)
    comment_df['comment_Published_At'] = pd.to_datetime(comment_df['comment_Published_At'], format="%Y-%m-%dT%H:%M:%SZ")
    return comment_df

