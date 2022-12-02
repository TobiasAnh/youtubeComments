#!/usr/bin/env python3
import pandas as pd

from youtubeComments import storage_path, importDFdtypes
from youtubeComments import getVideoIds, getVideoStatistics, getCommentsFromVideos
from youtubeComments import first_key

# General question
# Update the complete dataset? In theory, all videos can be commented at any time. Thus, everything
# Can be updated 

# Import
videos = pd.read_csv(storage_path.joinpath("videos.csv"), 
                       index_col = 0, 
                       lineterminator="\r")
videos = videos.astype(importDFdtypes("videos"))

channels = pd.read_csv(storage_path.joinpath("channels.csv"), 
                       index_col = 0, 
                       lineterminator="\r")
channels = channels.astype(importDFdtypes("channels"))

# Check existing videoIds
playlistIds = list(videos["playlistId"].unique())
playlistId = playlistIds[0]

playlistId_filter = videos["playlistId"] == playlistId
existing_videoIds = list(videos[playlistId_filter].index)

# Check current videoId list
fetched_videos = getVideoIds(playlistId, api_key_selector = first_key)
fetched_videoIds = fetched_videos["videoId"]

new_videos = fetched_videos.query("~videoId.isin(@existing_videoIds)")


