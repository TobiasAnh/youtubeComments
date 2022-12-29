import json
import numpy as np
import pandas as pd
from src.setup import storage_path, project_path
from src.setup import concatCommentsAndVideos, getChannelMetrics, exportDFdtypes

# =============================================================================
# Data import of csv files (the ones generated in the channel subfolders)
# =============================================================================

channel_paths = [x for x in storage_path.iterdir() if x.is_dir()]
print(f'found {len(channel_paths)} folders / channels.')

# first channel paths are excluded (some channels not yet relevant)
comments, videos = concatCommentsAndVideos(channel_paths[1:])

# Assign / create features
comments["owner_comment"] = comments["comment_author"] == comments["videoOwnerChannelTitle"]
comments["comment_word_count"] = comments["comment_string"].apply(lambda x: len(str(x).split()))
comments["comments_published_year"] = pd.DatetimeIndex(comments["comment_published"]).year
comments["response_time"] = (pd.to_datetime(comments["comment_published"]) -
                             pd.to_datetime(comments["publishedAt"]))

# =============================================================================
# Feature engineering (video)
# Available and removed comments (in total and in percent)
# =============================================================================
available_comments = comments.groupby("videoId", dropna=False).size()
available_comments.name = "available_comments"

videos = videos.join(available_comments).sort_values("available_comments")
videos["removed_comments"] = (videos["commentCount"] - videos["available_comments"])
videos["removed_comments_perc"] = videos["removed_comments"] / videos["commentCount"] * 100

# =============================================================================
# Feature engineering (video)
# Median comment length
# =============================================================================
user_comments = comments.query("owner_comment == False")
videos["mean_word_count"] = user_comments.groupby("videoId").agg({"comment_word_count":"median"})

# =============================================================================
# Feature engineering (video)
# Moderation activity per video (owner_comments per 1000 user comments)
# =============================================================================
moderation_activity = comments.groupby(["videoId", "owner_comment"]).size().reset_index()
moderation_activity = (moderation_activity
                      .pivot(index = "videoId", columns = "owner_comment", values = 0)
                      .reset_index())

moderation_activity = moderation_activity.T.reset_index(drop=True).T
moderation_activity.columns = ["videoId", "False", "True"]
moderation_activity["mod_activity"] = (
    moderation_activity["True"] / (moderation_activity["False"] + moderation_activity["True"])
    * 1000
)

moderation_activity = moderation_activity.set_index("videoId")

videos = videos.join(moderation_activity["mod_activity"]) 
# =============================================================================
# Feature engineering (video)
# Ratio: Replies over top_level_user_comments
# =============================================================================
videos["n_toplevel_user_comments"] = (
    user_comments.query("top_level_comment == True")
   .groupby("videoId")
   .size()
)
videos["n_user_replies"] = (
    user_comments.query("top_level_comment == False")
    .groupby("videoId")
    .size()
)

videos["ratio_RepliesToplevel"] = (videos["n_user_replies"] /
                                   videos["n_toplevel_user_comments"] )

# =============================================================================
# Feature engineering (video)
# Polarity (derived from sentiment) 
# =============================================================================

sentiment_proportions = (user_comments
                        .query("top_level_comment == True")
                        .groupby(["videoId", "sentiment"]).size().reset_index())

n_toplevel_neutral = sentiment_proportions.pivot(index = "videoId", 
                                                 columns = "sentiment", 
                                                 values = 0)["['neutral']"]
n_toplevel_neutral.name = "n_toplevel_neutrals" 

videos = videos.join(n_toplevel_neutral)
videos["polarity"] = videos["n_toplevel_neutrals"] / videos["n_toplevel_user_comments"]

# =============================================================================
# Feature engineering (video)
# Sentiment-Index (separately for top_level_comments and replies)
# =============================================================================

# Transforming sentiment into numerical feature
comments["sentiment"] = (
                          comments["sentiment"]
                         .astype("category")
                         .cat.rename_categories({"['positive']": 1,
                                                 "['neutral']": 0.5,
                                                 "['negative']": 0})
                         .astype("double")
                        )

videos["toplevel_sentiment_mean"] = (
    comments.query("top_level_comment == True & owner_comment == False")
   .groupby("videoId")
   .agg({"sentiment": "mean",})
   .apply(lambda x: round(x, 3))
)

videos["replies_sentiment_mean"] = (
    comments.query("top_level_comment == False & owner_comment == False")
    .groupby("videoId")
    .agg({"sentiment": "mean"}).apply(lambda x: round(x, 3))
)

# Remove sentiment estimation for videos with insufficient comment amount 
videos["toplevel_sentiment_mean"].mask(videos["n_toplevel_user_comments"] < 50,
                                       "NaN",
                                       inplace = True)

# =============================================================================
# Feature engineering (video)
# Responsivity
# =============================================================================

# Estimate time series of channel existance
start = pd.Timedelta(0)
end  = user_comments["comment_published"].max() - pd.to_datetime(user_comments["publishedAt"].min())
day_slices = pd.Series([0], dtype = "timedelta64[ns]")

for d in np.arange((end - start).days)+1:
    _ = len(day_slices)
    day_slices.loc[_] = pd.Timedelta(d, unit = "day")

user_comments["response_day"] = pd.cut(user_comments["response_time"],
                                       bins = day_slices,
                                       include_lowest= True,
                                       labels = np.arange(len(day_slices)-1),
                                       ordered = True)

comments_4weeks = user_comments[user_comments["response_day"] <= 27]
filter_1st_day = comments_4weeks["response_day"] < 1
comments_1st_day = comments_4weeks[filter_1st_day].groupby("videoId").size()
comments_1st_day.name = "comments_1st_day"
videos["responsivity"] = (comments_1st_day / comments_4weeks.groupby("videoId").size())

# =============================================================================
# Feature engineering (video)
# Likes and comments per 1000 views
# =============================================================================
videos["likes_per_1kViews"] = videos["likeCount"] / videos["viewCount"] * 1000
videos["comments_per_1kViews"] = videos["commentCount"] / videos["viewCount"] * 1000

# =============================================================================
# Feature engineering (video)
# Comments per author
# =============================================================================
comments.groupby("videoId").agg({"comment_author": "size"})
_ = user_comments.groupby(["videoId", "comment_author"]).size().reset_index()
_ = (_.rename(columns={0: 'comments_per_author'})
      .sort_values(["videoId", "comments_per_author"], ascending = False))

comments_per_author = _.groupby("videoId").agg({"comments_per_author" : "mean"})
videos = videos.join(comments_per_author)

# =============================================================================
# Add video url
# =============================================================================
URL_PREFIX = "https://www.youtube.com/watch?v="
videos["video_url"] =  URL_PREFIX + videos.index

# =============================================================================
# Convert YT categories
# =============================================================================

# NOTE: categories can be fetched from the API as well
# id_dict = dict()
# for item in response["items"]:
#     id_dict.update({item["id"]: item["snippet"]["title"]})

with open(project_path.joinpath("youtube_categories.json"), 'r') as filepath:
    categories_dict = json.load(filepath)

videos["categoryId"] = videos["categoryId"].apply(str)
videos["categoryId"].replace(categories_dict, inplace=True)

# =============================================================================
# Dataframe cleanups
# =============================================================================

comments["response_time_sec"] = comments["response_time"].dt.total_seconds()
comments["duration_sec"] = pd.to_timedelta(comments["duration_sec"]).dt.total_seconds()
comments = comments.drop(columns=["response_time", "duration_sec"], axis = 1)
comments = comments.set_index("comment_id")

videos["removed_comments_perc"] = round(videos["removed_comments_perc"], 1)
videos["likes_per_1kViews"] = round(videos["likes_per_1kViews"], 1)
videos["comments_per_1kViews"] = round(videos["comments_per_1kViews"], 1)
videos["responsivity"] = round(videos["responsivity"] * 100, 1)
videos["duration"] = pd.to_timedelta(videos["duration"]).dt.total_seconds()

convert_dict = {"mod_activity" : float,
                "toplevel_sentiment_mean" : float,
                "duration" : int}
videos = videos.astype(convert_dict)

# =============================================================================
# Channel-wide features
# =============================================================================

channelIds = list(videos["videoOwnerChannelId"].unique())
channels = pd.DataFrame()

for channelId in channelIds:
    _ = getChannelMetrics(channelId, "1")[0]
    _ = pd.DataFrame.from_dict(_, orient = "index").T
    channels = pd.concat([channels, _], axis = 0)

channels = channels.set_index("channelId")

agg_dict = {
      "video_url" : "size",
      "commentCount" : "sum",
      "available_comments" : "sum",
      "removed_comments": "sum",
      "likes_per_1kViews" : "mean",
      "comments_per_1kViews": "mean",
      "comments_per_author" : "mean",
      "mean_word_count" : "mean",
      "mod_activity": "mean",
      "responsivity" : "mean",
      "toplevel_sentiment_mean": "mean", # Note: simple average here, no weights
      "ratio_RepliesToplevel" : "mean"
}

channels_metrics = videos.groupby("videoOwnerChannelId").agg(agg_dict)

channels_metrics["removed_comments_perc"] = (
     channels_metrics["removed_comments"] /
    (channels_metrics["available_comments"] + channels_metrics["removed_comments"])
)

channels = pd.concat([channels, channels_metrics], axis = 1)
channels = channels.rename(columns = {"video_url":"n_videos"})

# =============================================================================
# Exports
# DataFrames to csv, dtypes to json
# =============================================================================

# Channels
channels.to_csv(storage_path.joinpath("channels.csv"), lineterminator="\r", index="videoOwnerChannelId")
exportDFdtypes(channels, "channels")

# Videos
videos.to_csv(storage_path.joinpath("videos.csv"), lineterminator="\r", index="videoId")
exportDFdtypes(videos, "videos")

# Comments
comments.to_csv(storage_path.joinpath("comments.csv"), lineterminator="\r", index="comment_id")
exportDFdtypes(comments, "comments")