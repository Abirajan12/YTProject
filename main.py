import streamlit as st
from youtube import *
from mongodb import *
from mysql import *

    
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
            msg = insert_data_json_to_mongodb_cluster(data)
            st.sidebar.write(msg)

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
    query_df = get_Query_Result(query)
    if query_df is not None:
        st.dataframe(query_df)   
    
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
