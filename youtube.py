from googleapiclient.discovery import build

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
                    'dislike_count': item['statistics'].get('dislikeCount', '0'),
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

def get_playlist_Data(playlist_Id, video_Ids):

    playlist_Data = []    
    for i in range(len(video_Ids)):
            playlist_Data.append({'playlist_Id': playlist_Id, 'video_Id': video_Ids[i]})
   
    return playlist_Data

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

