#!/usr/bin/env python3
import os
import shutil
import pandas as pd
import json

from src.funcs import first_key, second_key, storage_path, project_path
from src.funcs import getChannelMetrics, getVideoIds, getVideoStatistics, getCommentsFromVideos

# =============================================================================
# Channel Ids overview
# =============================================================================
channels = pd.read_csv(project_path.joinpath("references", "channelIds.csv"))
print(channels)

# =============================================================================
# API fetch starts with channelId (manual input)
# Check quota limit here: # https://console.cloud.google.com/apis
# =============================================================================

if not first_key:
    print("no API key set")

# Import Option 1 using a channelId (preferred!) 
# Loads basic metrics and create own subfolder
channelId = "UC4zcMHyrT_xyWlgy5WGpFFQ"
channel_metrics, channel_foldername = getChannelMetrics(channelId, api_key_selector = first_key)
channel_path = storage_path.joinpath(channel_foldername)
channel_path.mkdir(exist_ok = True)
playlistId = channel_metrics.get("playlistId") # this list contains all videos uploaded by the channel owner

# Import Option 2 (using a playlidId)
# loads basic metrics and create own subfolder
# playlistId = "PL2QF6_2vxWihUzfhGizc3pB3d0_N49vQG"
# channel_foldername = "BILD_ViertelNachAcht"
# channel_path = storage_path.joinpath(channel_foldername)
# channel_path.mkdir(exist_ok = True)

# Generates "all_videos.csv" within channel folder 
raw_video_info = getVideoIds(playlistId, api_key_selector = first_key)
raw_video_info = raw_video_info[~raw_video_info["videoOwnerChannelId"].isna()]
getVideoStatistics(raw_video_info, channel_path, api_key_selector = first_key)

# Import videoIds back from local storage
all_videos = pd.read_csv(channel_path.joinpath("all_videos.csv"), 
                         index_col = "videoId", 
                         lineterminator="\r")
all_videos["channel_foldername"] = channel_foldername

# =============================================================================
# API comment extraction for given videoIds.
# =============================================================================

# Remove videos with no or disabled comments
videoIds = list(all_videos.query("commentCount.notnull()") # NULL when comments are disabled
                          .query("commentCount != 0")      # 0 comments written (includes also Livestreams)
                          .index)

# If final file already exists, do nothing, otherwise start OR continue comment fetch.
if (
    ("all_comments_noSentiment.csv" in os.listdir(channel_path)) or 
    ("all_comments_withSentiment.csv" in os.listdir(channel_path))
   ):
    print('--------------------')
    print('csv file found')
    print('Comment fetch appears finished')
    
else:
    print('--------------------')
    # Stores only videos where all comment have been extracted
    # Missing videosIds are dumped in local json file
    getCommentsFromVideos(videoIds, channel_path, api_key_selector = first_key)
    
    # Various reasons may lead to incomplete comment extraxtions and therefore incomplete videos
    
    # Sometines comments are simply not fetched (two times seems sufficent)
    if "missing_videos.json" in os.listdir(channel_path):
        runs = 2
        for i in range(runs):
            print(f"missing_videos.json found, try {i+1} ... ")
            with open(channel_path.joinpath("missing_videos.json"), 'r') as filepath:
                missing_videos = json.load(filepath)
        
            getCommentsFromVideos(missing_videos, channel_path, api_key_selector = first_key)

    # If still missing, most likely no quotas are left. Try with other API key.
    if "missing_videos.json" in os.listdir(channel_path):
        print("missing_videos.json found, try with other API KEY ... ")
        with open(channel_path.joinpath("missing_videos.json"), 'r') as filepath:
            missing_videos = json.load(filepath)
                
        getCommentsFromVideos(missing_videos, channel_path, api_key_selector = second_key) 
        
    # If STILL missing, come back after daily quota reset at 9am.
    if "missing_videos.json" in os.listdir(channel_path):
        print("missing_videos.json found, no quotas left, no API Keys left, come back another day ... ")

# =============================================================================
# If fetch complete (= no missing_videos.json), files are concatenated into one csv 
# including some augmentation of features from all_videos.csv 
# =============================================================================

if "missing_videos.json" not in os.listdir(channel_path):

    video_files = os.listdir(channel_path.joinpath("tmp"))
        
        # Concatenate 
    all_comments = pd.DataFrame()
    counter = 0

    for file in video_files:
        _ = pd.read_csv(channel_path.joinpath("tmp", file), 
                        index_col = 0, 
                        parse_dates = ["comment_published", "comment_update"],
                        lineterminator='\n')
        
        all_comments = pd.concat([all_comments, _], axis = 0)
        
        progress = round( counter / len(video_files), 3)
        print(f'Fraction concatenated: {progress}')
        
        counter += 1

    # Augment video features
    video_features = all_videos[["Title", "videoOwnerChannelTitle", "publishedAt"]]
    all_comments_aug = pd.merge(left = all_comments, 
                                right = video_features, 
                                how = "left", 
                                on = "videoId")

    del all_comments
        
    # Drop unnecessary columns
    all_comments_aug = all_comments_aug.drop(["comment_update"], axis = 1)

    # =============================================================================
    # Data export "all_comments_noSentiment.csv" (locally)
    # ... and removing tmp/ folder
    # =============================================================================

    last_comment = all_comments_aug["comment_published"].max() # <- latest comment!
    all_comments_aug.info()
    all_comments_aug.to_csv(channel_path.joinpath("all_comments_noSentiment.csv"), lineterminator="\r")
    print(f'all_comments_noSentiment.csv saved | last comment within data ----> {last_comment}')

    # -- User input --
    user_input = (input(f'{channel_foldername} | {len(all_comments_aug)} comments concatenated from {len(video_files)} videos. \
    Delete subfolder "tmp"? [Y/N]:'))

    if user_input == "Y":
        folder = channel_path.joinpath("tmp")
        shutil.rmtree(folder)        
        print("temporary folder deleted.")
    else:
        print("nothing done.")
    
else: 
    print("Comment concatenation aborted (missing_videos.json still exists)")