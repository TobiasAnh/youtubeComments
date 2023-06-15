import pandas as pd
import numpy as np
from pandas_profiling import ProfileReport
from datetime import datetime
import plotly.express as px
from src.funcs import generateSubfolders
from src.funcs import relabeling_dict, px_select_deselect
from src.funcs import importDFdtypes

# =============================================================================
# Load and prepare video data
# =============================================================================

interim_path, processed_path, reports_path = generateSubfolders()

# Import csv and assign correct dtypes (both videos and comments)
videos = pd.read_csv(processed_path.joinpath("videos.csv"), 
                     lineterminator="\r", 
                     index_col="videoId")
videos = videos.astype(importDFdtypes(processed_path, "videos.json"))

# Do not use comment_id as index here (replies have same comment_id as the comments they reply to)
comments = pd.read_csv(processed_path.joinpath("comments.csv"), 
                       lineterminator="\r", 
                       index_col=0) 
comments = comments.astype(importDFdtypes(processed_path, "comments.json"))

# Only videos published before report_deadline are included in report.
# (to avoid including videos with insufficient time to accumulate comments
report_deadline = "2023-04-01 00:00:00+00:00"
report_deadline_short = "Q1 2023"

# =============================================================================
# Video Filter: 
# 1) only comments before report_deadline
# =============================================================================

videos.query("available_comments < 50").groupby("videoOwnerChannelTitle").size().sum()
videos_cutoff = videos.query("publishedAt < @report_deadline")
info_of_used_filter = f'Videos und Kommentare und bis einschl. {report_deadline_short} ({len(videos_cutoff)} Videos und {len(comments)} Kommentare)'

# =============================================================================
# ProfileReport (only for videos, comments too big!)
# =============================================================================

#profile = ProfileReport(videos_cutoff, title="Pandas Profiling Report")
#profile.to_file(reports_path.joinpath("videos_report.html"))

# =============================================================================
# Time series of comment amount 
# =============================================================================

frequency = "M" # choose frequency D, W, M, Q
monthly_comments = (comments.query("comment_published < @report_deadline")
                            .groupby(pd.Grouper(key = "comment_published", freq=frequency))
                            .size())

timeseries_comments = px.line(monthly_comments,
                              template = "simple_white")

timeseries_comments.write_html(reports_path.joinpath("timeseries_comments.html"))

# =============================================================================
# Comment feature distributions (splitted by channel)
# NOTE: can also be splitted by category
# =============================================================================

features = ["available_comments", "toplevel_sentiment_mean", "mod_activity", "responsivity", "ratio_RepliesToplevel",
            "mean_word_count", "comments_per_author", "removed_comments_perc", "toplevel_neutrality",
            "ZDF_content_references"]

for feature in features:
    distributions_allVideos = px.strip(videos_cutoff.query("available_comments >= 50"), 
                                       x = feature, y = "videoOwnerChannelTitle",
                                       color = "videoOwnerChannelTitle",
                                       hover_data = ["publishedAt",
                                                     "Title", 
                                                     "n_toplevel_user_comments",
                                                     "viewCount", 
                                                     feature],
                                       template = "simple_white",
                                       #opacity = 0.5,
                                       title = f'ZDF YouTube-Videos | {info_of_used_filter}',
                                       labels = relabeling_dict)
    
    #distributions_allVideos.update_layout(px_select_deselect)
    #distributions_allVideos.update_layout(xaxis={'range': [0, 1]})
    
    distributions_allVideos.write_html(
        reports_path.joinpath(f"Verteilung_{relabeling_dict.get(feature).replace(' ','_')}.html")
        )

# =============================================================================
# SPLOM: Scatter Plot Matrix 
# =============================================================================

splom_title = f'Scatterplot Matrix verschiedener YT-Video Eigenschaften | {info_of_used_filter})'
video_features = [
    "likes_per_1kViews", 
    #"mod_activity",
    "ratio_RepliesToplevel", 
    #"responsivity", 
    #"viewCount",
    "toplevel_sentiment_mean",
    #"toplevel_neutrality",
    #"comments_per_author",
    "removed_comments_perc"
]

splom = px.scatter_matrix(videos_cutoff,
                          dimensions = video_features,
                          color = "videoOwnerChannelTitle",
                          hover_data= ["Title"],
                          template = "simple_white",
                          opacity = 0.4,
                          title = splom_title,
                          labels = relabeling_dict)

splom.update_layout(px_select_deselect)
splom.write_html(reports_path.joinpath("SPLOM.html"))

# Relationship between performance and sentiment-index?
r_squared_matrix_all = (videos_cutoff.corr(numeric_only= True)**2)
r_squared_matrix = (videos_cutoff[video_features].corr(numeric_only=True)**2)
round(r_squared_matrix["likes_per_1kViews"], 2)

# =============================================================================
# Single SCATTER PLOT
# sentiment vs. video kpi
# =============================================================================
kpi = "viewCount"
scatter_plot = px.scatter(videos_cutoff,  
                          y = "toplevel_sentiment_mean", x = kpi, 
                          #facet_col = "videoOwnerChannelTitle",
                          #facet_col_wrap=5, 
                          #size = "commentCount", # Maybe viewCount here?
                          #size_max = 55,
                          hover_data = ["Title", "n_toplevel_user_comments","viewCount", "duration"], 
                          template = "simple_white",
                          opacity = 0.8,
                          title = f'ZDF YT-Videos | Beliebtheit vs. Sentiment-Index (Kreisgröße = Views; nur Videos mit mind. {min_comments} Kommentaren und Veröffentlichung vor 1.8.22, {info_of_used_filter})',
                          labels = relabeling_dict)

scatter_plot.update_layout(px_select_deselect)
scatter_plot.write_html(reports_path.joinpath("scatter_plot.html"))

sentiment_vs_popularity = videos_cutoff[["videoOwnerChannelTitle", kpi, "toplevel_sentiment_mean"]]

for channel in sentiment_vs_popularity["videoOwnerChannelTitle"].unique():
    r_matrix = sentiment_vs_popularity[sentiment_vs_popularity["videoOwnerChannelTitle"] == channel].corr(numeric_only= True)
    r_squared_matrix = round(r_matrix ** 2, 3)
    print(channel, r_squared_matrix.iloc[0,1])

# =============================================================================
# Channels quarterly resolution
# =============================================================================

# Generate new path for quarterly reports 
quarter_path = reports_path.joinpath("Quartalszahlen")
quarter_path.mkdir(exist_ok = True, parents=True)

# Augment quarter (as float .1 = Q1, .2 = Q2, etc.)
videos_cutoff["quarter"] = (
    videos_cutoff["publishedAt"].dt.year + 
    (videos_cutoff["publishedAt"].dt.quarter / 10)
)

comments["quarter"] = (
    comments["comment_published"].dt.year + 
   (comments["publishedAt"].dt.quarter / 10)
    .apply(lambda x: round(x, 1))
)

# Loop through quarters and aggregate metrics 
quarters = sorted(list(videos_cutoff["quarter"].unique()))
channels_quarter = pd.DataFrame()

for quarter in quarters:

    video_derived_metrics = (
         videos_cutoff
        .query("quarter == @quarter")
        .groupby("videoOwnerChannelTitle")
        .agg({
              "quarter" : "median",
              "video_url" : "size", # column only used to sum videos (relabeled below) 
              "viewCount": "sum",  
              "likeCount" : "sum",
              "commentCount" : "sum",
              "available_comments": "sum",
              "removed_comments" : "sum",
              "removed_comments_perc" : "mean",
              "comments_per_author" : "mean",
              "ratio_RepliesToplevel" : "mean",
              "toplevel_neutrality": "mean",
              "responsivity" : "mean",
              "toplevel_sentiment_mean": "mean",
              "ZDF_content_references": "sum"
            })
    ).reset_index()

    comment_derived_metrics = (
         comments
        .query("quarter == @quarter")
        .groupby("videoOwnerChannelTitle")
        .agg({
              "videoId" : "size", # column only used to sum comments (relabeled below) 
              "comment_word_count" : "median",
              "owner_comment" : "sum",
            })
    ).reset_index().drop("videoOwnerChannelTitle", axis = 1)

    derived_metrics = pd.concat([video_derived_metrics, comment_derived_metrics], axis = 1)
    channels_quarter = pd.concat([channels_quarter, derived_metrics], axis = 0)
    
    print(f"estimated metrics for {quarter}")

# Clean up dataframe
channels_quarter["videoCount"] = channels_quarter["video_url"]
channels_quarter["commentCount"] = channels_quarter["videoId"]
channels_quarter["mod_activity"] = channels_quarter["owner_comment"] / channels_quarter["available_comments"] * 1000
channels_quarter["references_per_video"] = channels_quarter["ZDF_content_references"] / channels_quarter["videoCount"]
channels_quarter = channels_quarter.drop(["video_url", "videoId"], axis = 1).reset_index(drop = True)
channels_quarter["quarter_cat"] = pd.Categorical(channels_quarter["quarter"], categories=quarters, ordered=True)
channels_quarter["quarter_cat"] = channels_quarter["quarter_cat"].cat.rename_categories(lambda x: str(x).replace(".", " Q"))
channels_quarter = channels_quarter.dropna()

# Export table
channels_quarter = channels_quarter.sort_values(["videoOwnerChannelTitle", "quarter"]).reset_index(drop=True)
(channels_quarter.rename(columns = relabeling_dict)
                 .to_csv(processed_path.joinpath("Quartalszahlen.csv"), 
                                               lineterminator="\r",
                                               index = False))

channels_quarter["publishedAt"] = channels_quarter["publishedAt"].apply(lambda x: x.tz_localize(None))
(channels_quarter.rename(columns = relabeling_dict)
                 .to_excel(quarter_path.joinpath("Quartalszahlen.xlsx"),
                                                 sheet_name="Quartalszahlen"))

# Quarterly plots (for each feature a single plot)
# min_quarter = 2019.1
# channels_quarter_plot = channels_quarter.query("quarter >= @min_quarter")

features = [
    "available_comments",
    "toplevel_sentiment_mean", 
    "responsivity", 
    "mod_activity", 
    "removed_comments_perc", 
    "references_per_video"
    ]

for feature in features:
    quaterly_metrics = px.line(channels_quarter,  
                               x = "quarter_cat", y = feature, 
                               category_orders={"quarter_cat": channels_quarter["quarter_cat"].cat.categories},
                               color = "videoOwnerChannelTitle",
                               hover_data = ["videoCount"], 
                               template = "simple_white",
                               #opacity = 0.8,
                               markers = True,
                               title = f'ZDF YT-Videos | {relabeling_dict.get(feature)} (Quartalswerte)',
                               labels = relabeling_dict)

    quaterly_metrics.update_layout(px_select_deselect)
    quaterly_metrics.update_xaxes(title = None)
    file_name = f"Quartalsverlauf_{relabeling_dict.get(feature).replace(' ','_')}.html"
    #quaterly_metrics.update_xaxes(type='category')
    quaterly_metrics.write_html(quarter_path.joinpath(file_name))