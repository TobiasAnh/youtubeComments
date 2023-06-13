#!/usr/bin/env python3
import os
import shutil
import pandas as pd
import json
from pathlib import Path

from src.funcs import ytChannel

# =============================================================================
# Channel Ids overview
# =============================================================================
channels = pd.read_csv(Path(os.getcwd()).joinpath("references", "channelIds.csv"))
print(channels)

# =============================================================================
# API fetch starts with channelId (manual input)
# Check quota limit here: # https://console.cloud.google.com/apis
# =============================================================================

channel = ytChannel("UCeaNCdxZcQsNMf8dkIOhLPg")
channel.getVideoIds()
channel.getAndSaveVideoStatistics(check_existing=False)
channel.getAndSaveComments()
channel.saveComments()
""" 
# =============================================================================
# API comment extraction for given videoIds.
# =============================================================================

# Remove videos with no or disabled comments
videoIds = list(
    all_videos.query("commentCount.notnull()")  # NULL when comments are disabled
    .query("commentCount != 0")  # 0 comments written (includes also Livestreams)
    .index
)

# If final file already exists, do nothing, otherwise start OR continue comment fetch.
if ("all_comments_noSentiment.csv" in os.listdir(channel_path)) or (
    "all_comments_withSentiment.csv" in os.listdir(channel_path)
):
    print("--------------------")
    print("csv file found")
    print("Comment fetch appears finished")

else:
    if "missing_videos.json" not in os.listdir(channel_path):
        getCommentsFromVideos(videoIds, channel_path, api_key_selector=first_key)

    else:
        # Various reasons may lead to incomplete comment extraxtions and therefore incomplete videos
        # Sometines comments are simply not fetched (two times seems sufficent)
        runs = 2
        for i in range(runs):
            print(f"missing_videos.json found, try {i+1} ... ")
            with open(channel_path.joinpath("missing_videos.json"), "r") as filepath:
                missing_videos = json.load(filepath)

            getCommentsFromVideos(
                missing_videos, channel_path, api_key_selector=first_key
            )

    # If still missing, most likely no quotas are left. Switch to second API key.
    if "missing_videos.json" in os.listdir(channel_path):
        print("missing_videos.json found, try with other API KEY ... ")
        with open(channel_path.joinpath("missing_videos.json"), "r") as filepath:
            missing_videos = json.load(filepath)

        getCommentsFromVideos(missing_videos, channel_path, api_key_selector=second_key)

    # If STILL missing, come back after daily quota reset at 9am.
    if "missing_videos.json" in os.listdir(channel_path):
        print(
            "missing_videos.json found, no quotas left, no API Keys left, come back another day ... "
        )

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
        _ = pd.read_csv(
            channel_path.joinpath("tmp", file),
            index_col=0,
            parse_dates=["comment_published", "comment_update"],
            lineterminator="\n",
        )

        all_comments = pd.concat([all_comments, _], axis=0)

        progress = round(counter / len(video_files), 3)
        print(f"Fraction concatenated: {progress}")

        counter += 1

    # Augment video features
    video_features = all_videos[["Title", "videoOwnerChannelTitle", "publishedAt"]]
    all_comments_aug = pd.merge(
        left=all_comments, right=video_features, how="left", on="videoId"
    )

    del all_comments

    # Drop unnecessary columns
    all_comments_aug = all_comments_aug.drop(["comment_update"], axis=1)

    # =============================================================================
    # Data export "all_comments_noSentiment.csv" (locally)
    # ... and removing tmp/ folder
    # =============================================================================

    last_comment = all_comments_aug["comment_published"].max()  # <- latest comment!
    all_comments_aug.info()
    all_comments_aug.to_csv(
        channel_path.joinpath("all_comments_noSentiment.csv"), lineterminator="\r"
    )
    print(
        f"all_comments_noSentiment.csv saved | last comment within data ----> {last_comment}"
    )

    # -- User input --
    user_input = input(
        f'{channel_foldername} | {len(all_comments_aug)} comments concatenated from {len(video_files)} videos. \
    Delete subfolder "tmp"? [Y/N]:'
    )

    if user_input == "Y":
        folder = channel_path.joinpath("tmp")
        shutil.rmtree(folder)
        print("temporary folder deleted.")
    else:
        print("nothing done.")

else:
    print("Comment concatenation aborted (missing_videos.json still exists)")
 """
