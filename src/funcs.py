#!/usr/bin/env python3
import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build

# load .env entries as environment variables
# NOTE: .env needs to be filled manually
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
first_key = os.environ.get("API_KEY_1")
second_key = os.environ.get("API_KEY_2")

# Define locations for downloads and reports) 
project_path = Path(os.getcwd())

data_path = project_path.joinpath("data")
data_path.mkdir(exist_ok=True)

storage_path = data_path.joinpath("interim")
storage_path.mkdir(exist_ok=True)

processed_path = data_path.joinpath("processed")
processed_path.mkdir(exist_ok=True)

reports_path = data_path.joinpath("reports") 
reports_path.mkdir(exist_ok = True)

# Functions for YouTube API requests
def setupYouTube(api_key_selector):

    """
    Build YouTube instance (Version 3) with account-related API key.
    Requires API key stored in /project_path/.env
    
            Parameters:
                api_key_selector (str): choose between 'first_key' or 'second_key'
            Return: 
                youtube (googleapiclient.discovery.Resource): youtube request instance
    """
    
    youtube = build(serviceName="youtube", version="v3", developerKey = api_key_selector)
    return youtube

def getChannelMetrics(channelId, api_key_selector):

    """
    Requests basic channel metrics including upload playlist.
    
            Parameters:
                    channelId (str): valid YouTube channelId    
                    api_key_selector (str): choose between 'first_key' or 'second_key'
            Returns:
                    channel (dict): stores basic channel info
                    channel_foldername (str): name used to create local folder
    """
    
    # Request channel infos
    youtube = setupYouTube(api_key_selector)
    channel_request = youtube.channels().list(part="snippet, statistics, contentDetails", id=channelId)
    channel_response = channel_request.execute()
    
    # Strore channel metrics in dictionary
    channel = dict()
    channel["channelId"] = channelId
    channel["channelTitle"] = channel_response["items"][0]["snippet"]["title"]
    channel["publishedAt"] = pd.to_datetime(channel_response["items"][0]["snippet"]["publishedAt"])
    channel["playlistId"] = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    channel["channel_foldername"] = channel["channelTitle"].replace(" ", "_").replace("&", "")
    statistics = channel_response["items"][0]["statistics"]
    channel.update(statistics)
    
    return channel, channel["channel_foldername"]

def getVideoIds(playlistId, api_key_selector): 
    
    """
    Requests basic video information from Youtube playlistId.
    Returns dataframe required for getVideoStatistics()
    
            Parameters:
                    playlistId (str): valid YouTube playlistId (e.g. upload playlist)     
                    api_key_selector (str): choose between 'first_key' or 'second_key'
            Returns:
                    raw_video_info (DataFrame): videoIds from given playlists and some additional info                                   
    """
    
    # Instantiate youtube instance with given api_key_selector
    youtube = setupYouTube(api_key_selector)
    
    # Data frame populated in while loop below
    raw_video_info = pd.DataFrame(columns=["videoId", "Title", "playlistId", 
                                           "videoOwnerChannelId", "videoOwnerChannelTitle"])

    # Loop breaks when no nextPageToken is generated
    nextPageToken = None
    while True:

        # Set up request
        videos_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlistId,
            maxResults=50,
            pageToken=nextPageToken,
        )

        # Execute request
        videos_response = videos_request.execute()

        # Loop through "items" to get videoId and title
        for item in videos_response["items"]:
                
            video = dict()
            video["videoId"] = item["snippet"]["resourceId"]["videoId"]
            video["Title"] = item["snippet"]["title"]
            video["playlistId"] = item["snippet"]["playlistId"]
            
            # Below required since these information are not existent when ...
            # ... videos are "private"
            if "videoOwnerChannelId" in item["snippet"]:
                video["videoOwnerChannelId"] = item["snippet"]["videoOwnerChannelId"]
                video["videoOwnerChannelTitle"] = item["snippet"]["videoOwnerChannelTitle"]
            
            _ = len(raw_video_info)
            raw_video_info.loc[_] = video

        nextPageToken = videos_response.get("nextPageToken")

        if not nextPageToken:
            break
        
    print(f'{len(raw_video_info)} videos found for {video["videoOwnerChannelTitle"]}')        
    return raw_video_info

def getVideoStatistics(raw_video_info, channel_path, api_key_selector):
    
    """
    Augments additional video metrics to DataFrame generated by getVideoIds().
    
            Parameters:
                    raw_video_info (DataFrame): contains videoIds required for loop 
                    channel_path (list): channel-specific folder path   
                    api_key_selector (str): choose between 'first_key' or 'second_key'
            Returns:
                    No returns. video metrics are stored in /{channel_path}/all_videos.csv                                
    """
    
    # Instantiate youtube instance with given api_key_selector
    youtube = setupYouTube(api_key_selector)
    
    # Data frame filled in loop below
    all_metrics = pd.DataFrame(
        columns=[
            "viewCount",
            "likeCount",
            "commentCount",
            "duration",
            "definition",
            "publishedAt",
            "description",
            "categoryId",
            "videoId",
        ]
    )
    
    # Variables for loop reporting
    counter, d = 0,0
    
    for videoId in raw_video_info["videoId"]:
    
        video_request = youtube.videos().list(
            part="snippet, contentDetails, statistics", id=videoId
        )
    
        video_response = video_request.execute()
    
        if video_response["items"]:
    
            metrics = video_response["items"][0]["statistics"]
            metrics["duration"] = video_response["items"][0]["contentDetails"]["duration"]
            metrics["definition"] = video_response["items"][0]["contentDetails"]["definition"]
            metrics["publishedAt"] = pd.to_datetime(video_response["items"][0]["snippet"]["publishedAt"])
            metrics["description"] = video_response["items"][0]["snippet"]["description"]
            metrics["categoryId"] = video_response["items"][0]["snippet"]["categoryId"]
            metrics["videoId"] = video_response["items"][0]["id"]
    
        _ = len(all_metrics)
        all_metrics.loc[_] = metrics
        
        if counter > d * 10:
            print(f"Metrics added for over {d*10} videos")
            d += 1
        
        counter += 1
    
    # Concat original dataframe with requested metrics. Save dataframe as .csv
    all_videos = pd.merge(left=raw_video_info, right=all_metrics, on="videoId")
    (all_videos
     .set_index("videoId")
     .to_csv(channel_path.joinpath("all_videos.csv"), lineterminator="\r"))
    
    print(f"Generated 'all_videos.csv' in {channel_path}")
    youtube.close()
            
def getCommentsFromVideos(videoIds, channel_path, api_key_selector):
    """ 
    Requests YouTube video comments from videoId list. Comments stored in csv for each video.
    Executes n API requests (n = number of videoIds). If fetch incomplete, "missing_videos.json" is stored
    locally.
            
            Parameters:
                    videoId_list (list): list of valid YouTube videoIds
                    channel_path (PosixPath): channel-specific folder path
                    api_key_selector (str): choose between 'first_key' or 'second_key'
                    
            Returns:
                    No returns. If comment fetch incomplete "missing_videos.json" is stored locally
    
    """
    
    print('Getting channel comments ... ')
    
    # Instantiate youtube instance with given api_key_selector
    youtube = setupYouTube(api_key_selector)
    
    # Loop through videos
    for videoId in videoIds:
        
        columns_comments = ['videoId', 'comment_id', 'comment_author', 
                            'comment_likes', 'comment_replies', 
                            'comment_published', 'comment_update', 
                            'comment_string', 'reply_id', 'top_level_comment']
        
        video_comments = pd.DataFrame(columns = columns_comments)
        
        # Try/except part allows only to store a complete comment request per video
        nextPageToken = None
        try:
            # Loop breaks when no nextPageToken is generated
            while True: 
                comments_request = youtube.commentThreads().list(
                        part = 'replies, snippet', 
                        videoId = videoId,
                        maxResults = 50,
                        pageToken = nextPageToken,
                        textFormat = "plainText"
                )
                
                comments_response = comments_request.execute()
                    
                # Comments 
                for item in comments_response["items"]:
                    
                    comments = dict()
                    comments["videoId"] = item["snippet"]["videoId"]
                    comments["comment_id"] = item["snippet"]["topLevelComment"]["id"]
                    comments["comment_author"]= item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                    comments["comment_likes"] = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
                    comments["comment_replies"] = item["snippet"]["totalReplyCount"]
                    comments["comment_published"] = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    comments["comment_update"] = item["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
                    comments["comment_string"] = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                    comments["reply_id"] = "None"
                    comments["top_level_comment"] = True
                    
                    _ = len(video_comments)
                    video_comments.loc[_] = comments
                    
                    # Replies
                    if "replies" in item:
                        
                        for reply in item["replies"]["comments"]:
                            
                            replies = dict()
                            replies["videoId"] = reply["snippet"]["videoId"]
                            replies["comment_id"] = reply["snippet"]["parentId"]
                            replies["comment_author"] = reply["snippet"]["authorDisplayName"] 
                            replies["comment_likes"] = reply["snippet"]["likeCount"]
                            replies["comment_replies"] = "NaN" # 
                            replies["comment_published"] = reply["snippet"]["publishedAt"]
                            replies["comment_update"] = reply["snippet"]["updatedAt"]
                            replies["comment_string"] = reply["snippet"]["textDisplay"]
                            replies["reply_id"] = reply["id"] # parentId.replyId
                            replies["top_level_comment"] = False
                            
                            _ = len(video_comments)
                            video_comments.loc[_] = replies
                
                nextPageToken = comments_response.get('nextPageToken')    
                if not nextPageToken:
                    break
                
            # Save populated dataframe locally in temporary folder 
            channel_path.joinpath("tmp").mkdir(exist_ok=True)
            video_comments.to_csv(channel_path.joinpath("tmp", f'{videoId}.csv'), escapechar='|')  
            
            progress = f'Video {videoIds.index(videoId)+1} of {len(videoIds)}'
            print(f'{progress} {videoId} | {len(video_comments)} comments found')
        
        # If requests fails, give only info via print
        except:
            print(f'Comment requests incomplete for {videoId}')
        
    # Detect missing videos and save them locally in json
    _ = os.listdir(channel_path.joinpath("tmp"))
    fetched_videoIds = [os.path.splitext(x)[0] for x in _]
    missing_videos = list(set(videoIds).difference(set(fetched_videoIds)))
    
    if missing_videos:
        print(f'Comments fetch NOT complete. {len(missing_videos)} are missing.')
        with open(channel_path.joinpath("missing_videos.json"), 'w') as filepath:
            json.dump(missing_videos, filepath)
        
    # If fetch complete, remove missing_videos.json
    else:
        
        if "missing_videos.json" in os.listdir(channel_path):
            file = channel_path.joinpath("missing_videos.json")
            os.remove(file)
            print('Rerun succesfull. Comments of all videos fetched :)')
        else:
            print('Comments of all videos fetched :)')
            print()




# =============================================================================
# Outsourced functions - No API requests involved below
# Mainly concatenations and data restructuring
# =============================================================================


def concatCommentsAndVideos(channel_paths):
    
    """ 
    Turns various csv files back into DataFrames.
    Requires two csv's "all_comments_withSentiment.csv" and "all_videos.csv" 
    in each subfolder listed in channel_paths.
    
    Parameters:
            channel_paths (list): list of PosixPath's
            
    Returns:
            comments (DataFrame): concatenated comments found within channel_paths
            videos (DataFrame): concatenaed videos found within channel_paths
    """
    
    comments = pd.DataFrame()
    videos = pd.DataFrame()
    
    for channel_path in channel_paths:
        try:
            # Import all_comments...
            comments_per_channel = pd.read_csv(channel_path.joinpath("all_comments_withSentiment.csv"), 
                                               index_col = 0, lineterminator="\r",
                                               parse_dates = ["publishedAt", "comment_published"] )
            
            comments = pd.concat([comments, comments_per_channel], axis = 0)
        
            # Import all_videos 
            videos_per_channel = pd.read_csv(channel_path.joinpath("all_videos.csv"), 
                                             index_col = 0, lineterminator="\r",
                                             parse_dates = ["publishedAt"])
            
            videos = pd.concat([videos, videos_per_channel], axis = 0)
            
            print(f'comments and videos concatenated from {channel_path}')
            
        except FileNotFoundError:
            print("Required csv files not found in ... ")
            print(f"{channel_path}")
        
    return comments, videos
    

# =============================================================================
# Export and import of datatypes
# =============================================================================

def exportDFdtypes(df, jsonfile):
    """ 
    Exports data types from given data frame into json file
    See also importDFdtypes().
    
    Parameters:
            df (DataFrame): name of DataFrame
            jsonfile (str): Name of json file
    """
    
    df_dtypes = df.dtypes.astype(str).to_dict()
    
    with open(processed_path.joinpath(jsonfile), 'w') as f:
        json.dump(df_dtypes, f)
        
def importDFdtypes(jsonfile):
    """ 
    ImoportExports data types from given data frame into json file
    See also exportDFdtypes().
    
    Parameters:
            jsonfile (str): Name of json file
    Return:
            dict() with datatypes 
        
    """
    
    with open(processed_path.joinpath(jsonfile), 'r') as f:
        return json.load(f)
    
            
# =============================================================================
# Other outsourced stuff             
# =============================================================================
            
relabeling_dict = {
                   "channelTitle" : "YT-Kanal",
                   "publishedAt": "Veröffentlicht am",
                   "toplevel_sentiment_mean" : "Sentiment-Index",
                   "videoOwnerChannelTitle": "Kanal",
                   "likeCount": "Likes",
                   "likes_per_1kViews": "Beliebtheit (Likes pro 1000 Views)",
                   "n_toplevel_user_comments" : "Nutzerkommentare (ohne Replies)",
                   "n_user_replies" : "Nutzerreplies",
                   "viewCount": "Views",
                   "duration": "Videolänge",
                   "mod_activity":"Moderationsaktivität",
                   "responsivity" : "Responsivität",
                   "commentCount" : "Kommentaranzahl",
                   "replies_sentiment_mean" : "Sentiment-Index (Replies)",
                   "removed_comments_perc" : "gelöschte Kommentare [%]",
                   "comment_word_count" : "Anzahl Kommentare (median)",
                   "mean_word_count" : "Mittlere Kommentarlänge",
                   "comments_per_author": "Kommentare pro Autor",
                   "videoCount" : "Videoanzahl (gesamt)",
                   "subscriberCount" : "Abonennt*innen",
                   "available_comments" : "verfügb. Kommentare",
                   "removed_comments" : "gelöschte Kommentare",
                   "comments_per_1kViews": "Kommentare pro 1000 Views",
                   "categoryId" : "YouTube Kategorie",
                   "ratio_RepliesToplevel":"Reply Intensität (Reply / Toplevel Kommentare)"
                  }
            
# =============================================================================
# Plotly settings (include a button allowing select / deselect functionality)
# =============================================================================
px_select_deselect = dict(
    
    font = dict(size = 18),
    updatemenus=[
    
        dict(type = "buttons",
             direction = "left",
             buttons=list([
                           dict(args=["visible", "legendonly"],
                                label="Deselect All",
                                method="restyle"
                           ),
                           dict(args=["visible", True],
                                label="Select All",
                                method="restyle"
                           )
                         ]),
             pad={"r": 10, "t": 10},
             showactive=False,
             x=1, xanchor="right",
             y=1.1, yanchor="top"
             ),
     ],
)