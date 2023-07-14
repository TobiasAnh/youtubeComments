import matplotlib.pyplot as plt
import nltk
import pandas as pd
from wordcloud import WordCloud

from src.funcs import generateSubfolders

interim_path, processed_path, reports_path = generateSubfolders()
project_path = interim_path.parent.parent
# Gathering and defining stopwords prior to wordcloud creation
# Stopwords (common words with no/little meaning)
nltk.download("stopwords", quiet=True)
stopwords_nltk = nltk.corpus.stopwords.words("german")
stopwords_foundOnline = list(pd.read_csv(project_path.joinpath("references", "stopwords_de.csv")).columns)
own_stopwords = [
    "bzw",
    "etc",
    "einfach",
    "schon",
    "sieht",
    "halt",
    "genau",
    "ne",
    "eigentlich",
    "eher",
    "finde",
    "sagen",
    "sogar",
]
stopwords_combined = stopwords_foundOnline + stopwords_nltk + own_stopwords

channel_paths = [x for x in interim_path.iterdir() if x.is_dir()]

# Start generating wordclouds (loops through channel paths)
for channel_path in channel_paths:
    selected_comments_df = pd.read_csv(
        channel_path.joinpath("all_comments_withSentiment.csv"),
        index_col=0,
        lineterminator="\r",
        parse_dates=["publishedAt", "comment_published"],
    )

    channel_foldername = channel_path.name
    channelTitle = selected_comments_df["videoOwnerChannelTitle"][0]

    selected_comments_df["comment_string"] = selected_comments_df["comment_string"].apply(str)
    comments_aggregated = (
        selected_comments_df.set_index("comment_published").resample("M").agg({"comment_string": " ".join})
    ).squeeze()
    reports_path.joinpath(channel_foldername).mkdir(parents=True, exist_ok=True)

    # TODO does not work

    for idx, comment_agg in comments_aggregated.items():
        wordcloud_filtered = WordCloud(
            stopwords=stopwords_combined, background_color="white", margin=10, width=1024, height=768, min_word_length=3
        ).generate(str(comment_agg))

        plt.imshow(wordcloud_filtered, interpolation="bilinear")
        plt.suptitle(f"WordCloud | YT-Kommentare {channelTitle}")
        plt.title(f"{idx.year} {idx.month_name()}")
        plt.axis("off")
        plt.savefig(reports_path.joinpath(channel_foldername, f"WordCloud_{idx.year}_{idx.month}.png"), dpi=600)

        print(f"{channelTitle} | Wordcloud done for {idx.year} {idx.month_name()}")


# # Wordcloud for single video (differentiate between good bad sentoimetn)

selected_comments_df = pd.read_csv(
    channel_path.joinpath("all_comments_withSentiment.csv"),
    index_col=0,
    lineterminator="\r",
    parse_dates=["publishedAt", "comment_published"],
)

channel_foldername = channel_path.name
channelTitle = selected_comments_df["videoOwnerChannelTitle"][0]

negative_video = (
    selected_comments_df.groupby("videoId")
    .agg({"positive": "median", "videoId": "size"})
    .rename(columns={"videoId": "count"})
    .sort_values("positive")
    .head(5)
    .sort_values("count")
    .tail(1)
)
median_senti = negative_video["positive"].item()
negative_videoId = negative_video.index.item()
comments_singleVideo = selected_comments_df.query("videoId == @negative_videoId")
video_title = comments_singleVideo["Title"].iloc[1]
video_title_short = video_title[:35]
comments_for_wordcloud_filtered = comments_singleVideo[["comment_string", "positive"]]
comments_negative = comments_for_wordcloud_filtered.query("positive < @median_senti")
comments_negative = " ".join(comments_negative["comment_string"].apply(str))

wordcloud_filtered = WordCloud(
    stopwords=stopwords_combined, background_color="white", margin=10, width=1024, height=768
).generate(comments_negative)

plt.imshow(wordcloud_filtered, interpolation="bilinear")
plt.suptitle(f"{video_title_short} WordCloud")
plt.axis("off")
plt.savefig(
    reports_path.joinpath(channel_foldername, f"WordCloud_{channel_foldername}_{negative_videoId}.png"), dpi=600
)
