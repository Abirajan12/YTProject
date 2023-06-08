# Youtube Data Harvesting and Warehousing

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

## Table of Contents

    1. Installation
    2. Youtube API Key
    3. Usage
    4. Documentation
    5. Demo

## 1. Installation

On a high level note, the project has 4 sections for which the below packages are mandatory to install.

    UI:
    ---
    import streamlit as st

    Youtube API call:
    ------------------
    from googleapiclient.discovery import build

    MongoDB connection and Query:
    ---------------------------------
    from pymongo import MongoClient
    import pandas as pd
    import numpy as np

    mysqlDB connection and Query:
    -----------------------------
    from sqlalchemy import create_engine,inspect
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError

    To make sqlalchemy to work in vs code, it is mandatory to CREATE, SELECT, SET and ACTIVATE virtual environment.
    https://stackoverflow.com/questions/61944862/vs-code-not-able-to-recognize-sqlalchemy

## 2. Youtube API key

    1. Log in to Google Developers Console.
    2. Create a new project.
    3. On the new project dashboard, click Explore & Enable APIs.
    4. In the library, navigate to YouTube Data API v3 under YouTube APIs.
    5. Enable the API.
    6. Create a credential.
    7. A screen will appear with the API key.

    https://blog.hubspot.com/website/how-to-get-youtube-api-key

## 3. Usage

    This project has 3 major functionalities
        * Get Data
        * Migrate Data
        * Query Data
    1. Get Data:
        1. Asks for an API KEY from the user. (create API KEY using steps explained in the previous section)
        2. Input a youtube channel id(from the page source of the corresponding channel)
        3. once entered it gets all the data related to that channel in a json format and insert it into a mongoDB collection as a document

    channel_Data  = get_channel_Data(youtube, channel_Id)
    video_Ids     = get_video_IDs(youtube, channel_Data['playlist_Id'])
    playlist_Data = get_playlist_Data(channel_Data['playlist_Id'], video_Ids)
    video_Data    = get_video_Data(youtube,video_Ids)
    comment_Data  = get_comment_Data(youtube,video_Ids)

    insert_data_json_to_mongodb_cluster(data)

    2. Migrate Data:
        1. Display a drop down to select a list of channels that needs to be migrated from MongoDB to mysql.
        2. convert all the data to dataframes using

    get_channel_df(mig_Data)
    get_playlist_df(mig_Data)
    get_video_df(mig_Data)
    get_comment_df(mig_Data)
        3. And transform them in an appropriate format to insert into mysql database using

    transform_channel_df(channel_df)
    transform_playlist_df(playlist_df)
    transform_video_df(video_df)
    change_duration_datatype(video_df)
    transform_comment_df(comment_df)

    3. Query Data:
        Once the data got inserted in to four tables channel, playlist, video and comment it displays the query and the results

## 4. Documentation

[API reference ](https://blog.hubspot.com/website/how-to-get-youtube-api-key)

[Virtual environment creation ](https://stackoverflow.com/questions/61944862/vs-code-not-able-to-recognize-sqlalchemy)

[MongoDB Atlas cluster creation and connection string ](https://medium.com/analytics-vidhya/connecting-to-mongodb-atlas-with-python-pymongo-5b25dab3ac53#:~:text=Start%20by%20creating%20a%20new,connect%20to%20a%20MongoDB%20database.&text=Connect%20the%20cluster%20using%20the,and%20password%20in%20this%20URL.)

## 5. Demo

[Youtube Data Harvesting and Warehousing](https://www.linkedin.com/posts/abirami-rengarajan-4b017b27a_youtube-datascrapping-datascientist-activity-7072548039969665024-hISa?utm_source=share&utm_medium=member_desktop)
