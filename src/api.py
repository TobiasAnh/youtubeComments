#!/usr/bin/env python3
import os
import traceback
import json
import shutil
import re
import pandas as pd
import time
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build
from datetime import datetime

from src.funcs import generateSubfolders


def testAndGenerateInstance(developerKey: str):
    """
    Execute a test API request and return instance when succesful

    Parameter:
        developerKey: valid API key
    Returns:
        youtube request instance
    """
    youtube = build(serviceName="youtube", version="v3", developerKey=developerKey)
    testVideoId = "jNQXAC9IVRw"  # oldest YT video (just for testing purposes)
    channels_list_response = youtube.videos().list(part="snippet", id=testVideoId).execute()
    if "items" in channels_list_response:
        print("API key functional!")
        return youtube


# Functions for YouTube API requests
def setupYouTube():
    """
    Test and returns a YouTube instance (Version 3) by trying two API keys.
    Requires API keys stored in .env file in repository

    Return:
        youtube (googleapiclient.discovery.Resource): youtube request instance
    """

    # load .env entries as environment variables
    # NOTE: .env needs to be filled manually
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
    first_key = os.environ.get("API_KEY_1")
    second_key = os.environ.get("API_KEY_2")

    # Execute a API request for testing
    try:
        return testAndGenerateInstance(first_key)

    # If first API requests fails, switch to second API key
    except Exception as e:
        print(e)
        print("Trying another API Key")

        return testAndGenerateInstance(second_key)


class ytChannel:
    def __init__(self, channelId: str):
        """
        Initializes new instance that contains basic infos and metrics of a YouTube channel

                Parameters:
                    channelId (string): valid YouTube channelId.
        """

        validChannelId = bool(re.match(r"^[a-zA-Z0-9_-]{24}$", channelId))
        if not validChannelId:
            print("No valid YouTube channel ID provided")
            return

        info = dict()
        info["channelId"] = channelId

        # Request channel infos and stores as attributes
        youtube = setupYouTube()
        channel_request = youtube.channels().list(part="snippet, statistics, contentDetails", id=channelId)
        channel_response = channel_request.execute()
        channel_items = channel_response["items"][0]

        info["channelTitle"] = channel_items["snippet"]["title"]
        info["publishedAt"] = channel_items["snippet"]["publishedAt"]
        info["playlistId"] = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        info["channel_foldername"] = info["channelTitle"].replace(" ", "_").replace("&", "")
        info.update(channel_items["statistics"])

        self.info = info
        print(f"Loaded YT Channel: {self.info['channelTitle']}")
        print(self.info)

        # Subsubfolder for chosen channel
        (
            self.interim_path,
            self.processed_path,
            self.reports_path,
        ) = generateSubfolders()
        self.channel_path = self.interim_path.joinpath(self.info["channel_foldername"])
        self.channel_path.mkdir(exist_ok=True, parents=True)

        channel_info_file = self.channel_path.joinpath("channel_info")
        with open(channel_info_file, "w") as file:
            json.dump(info, file)

        print()
        print(f"Stored channel info stored in {channel_info_file}")
        print()

    def fetchVideoIds(self):
        """
        Requests basic video information from Youtube playlistId.
        Assign DataFrame to object attribute (required for further analysis)
        """

        # Instantiate youtube instance
        youtube = setupYouTube()

        # Data frame populated in while loop below
        raw_video_info = pd.DataFrame(
            columns=[
                "videoId",
                "Title",
                "playlistId",
                "videoOwnerChannelId",
                "videoOwnerChannelTitle",
            ]
        )

        # Loop breaks when no nextPageToken is generated
        nextPageToken = None
        while True:
            # Set up request
            videos_request = youtube.playlistItems().list(
                part="snippet",
                playlistId=self.info["playlistId"],
                maxResults=50,
                pageToken=nextPageToken,
            )

            # Execute request and loop through items
            videos_response = videos_request.execute()

            for item in videos_response["items"]:
                video = dict()
                item_snippet = item["snippet"]
                video["videoId"] = item_snippet["resourceId"]["videoId"]
                video["Title"] = item_snippet["title"]
                video["playlistId"] = item_snippet["playlistId"]

                # Below required since these information are not existent when ...
                # ... videos are "private"
                if "videoOwnerChannelId" in item_snippet:
                    video["videoOwnerChannelId"] = item_snippet["videoOwnerChannelId"]
                    video["videoOwnerChannelTitle"] = item_snippet["videoOwnerChannelTitle"]

                _ = len(raw_video_info)
                raw_video_info.loc[_] = video

            nextPageToken = videos_response.get("nextPageToken")

            if not nextPageToken:
                break

        self.raw_video_info = raw_video_info

        print(f'{len(raw_video_info)} videos found for {video["videoOwnerChannelTitle"]}')
        print()

    def fetchAndStoreVideoStatistics(self, load_existing: bool):
        """
        Augments additional video metrics and save as .csv file.
        videoIds are used fromUses self.raw_video_info

                Parameters:
                        use_existing (bool): Use existing .csv file to continue analysis.
                        If no file found, fresh data is loaded.
                Returns:
                        No returns. video metrics are stored in /{channel_path}/all_videos.csv
        """

        if load_existing:
            try:
                # Import videoIds back from local storage
                all_videos = pd.read_csv(
                    self.channel_path.joinpath("all_videos.csv"),
                    index_col="videoId",
                    lineterminator="\r",
                    parse_dates=["publishedAt"],
                )
                all_videos["channel_foldername"] = self.info["channel_foldername"]

                print("Videos metrics already fetched and stored in all_videos.csv")
                print(f"Most recent video fetched from {all_videos['publishedAt'].max()}")
                print("Set check_existing_videos=False to fetch fresh API data")
                return

            except FileNotFoundError as e:
                print("No existing file 'all_videos.csv' found.")
                return

        # Instantiate youtube instance
        youtube = setupYouTube()

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
        d = 0

        for counter, videoId in enumerate(self.raw_video_info["videoId"]):
            video_request = youtube.videos().list(part="snippet, contentDetails, statistics", id=videoId)

            video_response = video_request.execute()

            if video_response["items"]:
                metrics = dict()
                response_items = video_response["items"][0]
                metrics = response_items["statistics"]
                metrics["duration"] = response_items["contentDetails"]["duration"]
                metrics["definition"] = response_items["contentDetails"]["definition"]
                metrics["publishedAt"] = response_items["snippet"]["publishedAt"]
                metrics["publishedAt"] = pd.to_datetime(metrics["publishedAt"])
                metrics["description"] = response_items["snippet"]["description"]
                metrics["categoryId"] = response_items["snippet"]["categoryId"]
                metrics["videoId"] = response_items["id"]

            _ = len(all_metrics)
            all_metrics.loc[_] = metrics

            if counter > d * 10:
                print(f"Metrics added for over {d*10} videos")
                d += 1

        # Concat original dataframe with requested metrics. Save dataframe as .csv
        all_videos = pd.merge(left=self.raw_video_info, right=all_metrics, on="videoId")
        all_videos.set_index("videoId").to_csv(self.channel_path.joinpath("all_videos.csv"), lineterminator="\r")
        self.dateOfVideoFetch = datetime.now().date().today()
        print(f"Generated 'all_videos.csv' in {self.channel_path}")

    def fetchComments(self, load_existing: bool):
        """
        Requests YouTube video comments using videoIds list. videoIds are stored in self.raw_video_info.
        Comments are stored in tmp/ as csv for each video.

        If fetch incomplete, "missing_videos.json" is stored locally and method runs again.

        """

        # If final file already exists, do nothing, otherwise start OR continue comment fetch.
        if load_existing:
            if ("all_comments_noSentiment.csv" in os.listdir(self.channel_path)) or (
                "all_comments_withSentiment.csv" in os.listdir(self.channel_path)
            ):
                return f"Comment fetch appears finished. csv file found in {self.channel_path}. \
                        Set load_existing=False for a fresh comments fetch."

        if not "missing_videos.json" in os.listdir(self.channel_path):
            all_videos = pd.read_csv(
                self.channel_path.joinpath("all_videos.csv"),
                index_col="videoId",
                lineterminator="\r",
            )

            videoIds = list(
                all_videos.query("commentCount.notnull()")  # NULL when comments are disabled
                .query("commentCount != 0")
                .index  # 0 comments written (includes also Livestreams)
            )

            print("Starting fetch of channel comments ... ")

        else:
            with open(self.channel_path.joinpath("missing_videos.json"), "r") as filepath:
                videoIds = list(json.load(filepath))

            print("Continuing fetch with missing_videos.json")

        columns_comments = [
            "videoId",
            "comment_id",
            "comment_author",
            "comment_likes",
            "comment_replies",
            "comment_published",
            "comment_update",
            "comment_string",
            "reply_id",
            "top_level_comment",
        ]

        youtube = setupYouTube()

        # Loop through videos
        for counter, videoId in enumerate(videoIds):
            video_comments = pd.DataFrame(columns=columns_comments)

            # Try/except part allows only to store a complete comment request per video
            nextPageToken = None
            try:
                while True:
                    comments_request = youtube.commentThreads().list(
                        part="replies, snippet",
                        videoId=videoId,
                        maxResults=50,
                        pageToken=nextPageToken,
                        textFormat="plainText",
                    )

                    comments_response = comments_request.execute()

                    # comments and replies
                    for item in comments_response["items"]:
                        comments = dict()
                        item_snippet = item["snippet"]
                        comments["videoId"] = item_snippet["videoId"]
                        comments["comment_id"] = item_snippet["topLevelComment"]["id"]
                        comments["comment_author"] = item_snippet["topLevelComment"]["snippet"]["authorDisplayName"]
                        comments["comment_likes"] = item_snippet["topLevelComment"]["snippet"]["likeCount"]
                        comments["comment_replies"] = item_snippet["totalReplyCount"]
                        comments["comment_published"] = item_snippet["topLevelComment"]["snippet"]["publishedAt"]
                        comments["comment_update"] = item_snippet["topLevelComment"]["snippet"]["updatedAt"]
                        comments["comment_string"] = item_snippet["topLevelComment"]["snippet"]["textDisplay"]
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
                                replies["comment_replies"] = "NaN"  #
                                replies["comment_published"] = reply["snippet"]["publishedAt"]
                                replies["comment_update"] = reply["snippet"]["updatedAt"]
                                replies["comment_string"] = reply["snippet"]["textDisplay"]
                                replies["reply_id"] = reply["id"]  # parentId.replyId
                                replies["top_level_comment"] = False

                                _ = len(video_comments)
                                video_comments.loc[_] = replies

                    nextPageToken = comments_response.get("nextPageToken")
                    if not nextPageToken:
                        break

                # Save populated dataframe locally in temporary folder
                self.channel_path.joinpath("tmp").mkdir(exist_ok=True)

                video_comments.to_csv(
                    self.channel_path.joinpath("tmp", f"{videoId}.csv"),
                    escapechar="|",
                )

                progress = f"Video {counter+1} of {len(videoIds)}"
                print(f"{progress} {videoId} | {len(video_comments)} comments found")

            except Exception as e:
                # Code to handle any exception and display error details
                error_type = type(e).__name__
                error_msg = str(e)
                error_traceback = traceback.format_exc()
                print(f"Error: {error_type} - {error_msg}")
                print(f"Module/File: {error_traceback}")

        # Check for missing videos
        _ = os.listdir(self.channel_path.joinpath("tmp"))
        fetched_videoIds = [os.path.splitext(x)[0] for x in _]
        missing_videos = list(set(videoIds).difference(set(fetched_videoIds)))

        if missing_videos:
            print(f"Comments fetch NOT complete. {len(missing_videos)} are missing.")
            with open(self.channel_path.joinpath("missing_videos.json"), "w") as filepath:
                json.dump(missing_videos, filepath)

            self.getComments()  # start method again

        # If no missing videos (fetch complete), remove missing_videos.json
        else:
            if "missing_videos.json" in os.listdir(self.channel_path):
                os.remove("missing_videos.json")
            print("Comments of all videos fetched :)")
            print()

    def concatAndSaveComments(self):
        """
        Concatenates comments from each video (stored in tmp/) into a single DataFrame.
        DataFrame is then stored as csv and the tmp/ is deleted.

        """

        if "tmp" not in os.listdir(self.channel_path):
            return "No tmp subfolder found. Nothing to concatenate."

        print("Concatenating .csv files from tmp subfolder")
        all_comments = pd.DataFrame()
        video_files = os.listdir(self.channel_path.joinpath("tmp"))

        for counter, file in enumerate(video_files):
            _ = pd.read_csv(
                self.channel_path.joinpath("tmp", file),
                index_col=0,
                parse_dates=["comment_published", "comment_update"],
                lineterminator="\n",
            )

            all_comments = pd.concat([all_comments, _], axis=0)

            progress = round(counter / len(video_files), 3)
            print(f"Fraction concatenated: {progress}")

        # Merge video features to comments
        all_videos = pd.read_csv(
            self.channel_path.joinpath("all_videos.csv"),
            index_col="videoId",
            lineterminator="\r",
        )
        video_features = all_videos[["Title", "videoOwnerChannelTitle", "publishedAt"]]
        all_comments = pd.merge(left=all_comments, right=video_features, how="left", on="videoId")

        # Drop unnecessary columns
        all_comments = all_comments.drop(["comment_update"], axis=1)

        # Save as .csv
        file_concatenated_comments = "all_comments_noSentiment.csv"
        all_comments.to_csv(
            self.channel_path.joinpath(file_concatenated_comments),
            lineterminator="\r",
        )

        # Assign comments to channel object
        self.comments = all_comments

        print(
            f"{self.info['channel_foldername']} | {len(all_comments)} comments concatenated from {len(video_files)} videos."
        )
        if file_concatenated_comments in os.listdir(self.channel_path):
            folder = self.channel_path.joinpath("tmp")
            shutil.rmtree(folder)
            print("temporary folder deleted.")

    def applyGermanSentibert(self):
        """
        Executes sentiment analysis using the model "germansentiment".

        Parameters:
                channel_path (list): local folder for importing required .csv file
        Returns:
                No returns. Sentiment is appended to DataFrame which is stored again as csv file
                in channel_path
        """

        # Import all_comments_noSentiment.csv from channel folder
        try:
            comments_for_sentiment = pd.read_csv(
                self.channel_path.joinpath("all_comments_noSentiment.csv"),
                index_col=0,
                lineterminator="\r",
                parse_dates=["publishedAt", "comment_published"],
                dtype={"comment_string": "str"},
            )
            print("Comments found for sentiment analysis.")
        except FileNotFoundError as e:
            return e

        print("loading model ...")
        from germansentiment import SentimentModel

        sentiment = SentimentModel()

        # Loop for sentiment analysis
        n_comments = len(comments_for_sentiment)  # required for loop reporting
        sentiment_estimate = ()
        sentimentsDF = pd.DataFrame()  # sentiments go here
        print(f"Analyzing sentiment for {n_comments} comments fetched from {self.info['channelTitle']} ...")
        start = time.time()

        for comment in comments_for_sentiment["comment_string"]:
            _ = pd.DataFrame()

            sentiment_estimate = sentiment.predict_sentiment([str(comment)], True)
            _["prediction"] = sentiment_estimate[0]
            _["positive"] = sentiment_estimate[1][0][0][1]
            _["negative"] = sentiment_estimate[1][0][1][1]
            _["neutral"] = sentiment_estimate[1][0][2][1]

            sentimentsDF = pd.concat([sentimentsDF, _], ignore_index=True)
            fraction_done = len(sentimentsDF) / n_comments

            # loop reporting (in per mil steps)
            if (fraction_done) > 0.001:
                time_per_pm = time.time()
                time_passed = time_per_pm - start
                print(
                    f"{self.info['channelTitle']} | {round(fraction_done, 3)} done | time per loop: {round(time_passed, 3)}"
                )
                start = time.time()

        # Augment sentiment to original data frame and store as csv
        comments_for_sentiment = pd.merge(comments_for_sentiment, sentimentsDF, left_index=True, right_index=True)
        comments_for_sentiment.to_csv(
            self.channel_path.joinpath("all_comments_withSentiment.csv"),
            lineterminator="\r",
        )

        # if new csv file exists, delete old one
        if "all_comments_withSentiment.csv" in os.listdir(self.channel_path):
            file = self.channel_path.joinpath("all_comments_noSentiment.csv")
            os.remove(file)
            print("original csv deleted.")
