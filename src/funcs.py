#!/usr/bin/env python3
import os
import json
import re
import pandas as pd
from pathlib import Path

# =============================================================================
# Outsourced functions - No API requests involved below
# Mainly concatenations and data restructuring
# =============================================================================


def generateSubfolders():
    """
    Generates subfolder structure user later for storage of data, mostly .csv files

        Return: Paths of interim, processed and reports subfolders
    """

    project_path = Path(os.getcwd())

    # data path
    data_path = project_path.joinpath("data")
    data_path.mkdir(exist_ok=True, parents=True)

    # Subfolders
    interim_path = data_path.joinpath("interim")
    processed_path = data_path.joinpath("processed")
    reports_path = data_path.joinpath("reports")

    for subfolder in ["interim", "processed", "reports"]:
        data_path.joinpath(subfolder).mkdir(exist_ok=True, parents=True)

    return interim_path, processed_path, reports_path


def concatChannelData(channel_paths):
    """
    Concatenates channel data into comprehensive DataFrames.
    Uses each channel with existing subfolder found in interim/

    Parameters:
            channel_paths (list): list of PosixPath's

    Returns:
            channels (DataFrame): concatenated channel infos found within channel_paths
            comments (DataFrame): concatenated comments found within channel_paths
            videos (DataFrame): concatenaed videos found within channel_paths
    """
    channels = pd.DataFrame()
    comments = pd.DataFrame()
    videos = pd.DataFrame()

    for channel_path in channel_paths:
        try:
            # Import all channels
            with open(channel_path.joinpath("channel_info"), "r") as file:
                data = json.load(file)

            channel = pd.DataFrame.from_dict([data])
            channels = pd.concat([channels, channel], axis=0)

            # Import all_videos
            videos_per_channel = pd.read_csv(
                channel_path.joinpath("all_videos.csv"),
                index_col=0,
                lineterminator="\r",
                parse_dates=["publishedAt"],
            )

            videos = pd.concat([videos, videos_per_channel], axis=0)

            # Import all_comments
            comments_per_channel = pd.read_csv(
                channel_path.joinpath("all_comments_withSentiment.csv"),
                index_col=0,
                lineterminator="\r",
                parse_dates=["publishedAt", "comment_published"],
            )

            comments = pd.concat([comments, comments_per_channel], axis=0)

            print(f"Loaded {channel_path}")

        except FileNotFoundError:
            print("Required files not found in ... ")
            print(f"{channel_path}")

    return channels, comments, videos


def findZDFurl(string):
    # TODO add description

    string = str(string).lower()

    # regex url detection
    regex = r"((?<=[^a-zA-Z0-9])(?:https?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*)"
    url = re.findall(regex, string)

    # return True when url references to ZDF (but not to the service section)
    if len(url) == 0:
        return False  # no url at all
    else:
        if str(url).find("zdf") != -1:
            if str(url).find("service") != -1:  # zdf url but reference to service site (netiquette)
                return False
            else:
                return True  # zdf url
        else:
            return False  # url but not ZDF


def determine_gender(author, male_names, female_names):
    try:
        author = str(author)
        author_firstname = author.split()[0].lower()
        pattern = r"[^a-zA-Z\s]"  # pattern matches any character that is not a letter
        author_firstname = re.sub(pattern, "", author_firstname)

        # check with name lists
        if author_firstname in male_names and author_firstname not in female_names:
            return "male"
        elif author_firstname not in male_names and author_firstname in female_names:
            return "female"
        # check if most informative features applies
        # https://www.geeksforgeeks.org/python-gender-identification-by-name-using-nltk/
        elif re.search(r"a$", author_firstname):
            return "female"
        elif re.search(r"k$", author_firstname):
            return "male"
        elif re.search(r"f$", author_firstname):
            return "male"
        else:
            return "undefined"
    except:
        return "undefined"


#
def subcategorizeChannel(text, category_mappings):
    for pattern in category_mappings.keys():
        if re.search(re.escape(pattern), text):
            return category_mappings[pattern]
    return "Other"


# used in subcategorizeChannel()
unbubble_mapping = {
    "13 Fragen": "13 Fragen",
    "s mir": "Sags mir",
    "Unter Anderen": "Unter Anderen",
}


# =============================================================================
# Export and import of datatypes
# =============================================================================


def exportDFdtypes(df, processed_path, jsonfile):
    """
    Exports data types from given data frame into json file
    See also importDFdtypes().

    Parameters:
            df (DataFrame): name of DataFrame
            jsonfile (str): Name of json file
    """

    df_dtypes = df.dtypes.astype(str).to_dict()

    with open(processed_path.joinpath(jsonfile), "w") as f:
        json.dump(df_dtypes, f)


def importDFdtypes(processed_path, jsonfile):
    """
    ImoportExports data types from given data frame into json file
    See also exportDFdtypes().

    Parameters:
            jsonfile (str): Name of json file
    Return:
            dict() with datatypes

    """

    with open(processed_path.joinpath(jsonfile), "r") as f:
        return json.load(f)


# =============================================================================
# Other outsourced stuff
# =============================================================================

relabeling_dict = {
    "channelTitle": "YT-Kanal",
    "publishedAt": "Veröffentlicht am",
    "toplevel_sentiment_mean": "Sentiment-Index",
    "videoOwnerChannelTitle": "Kanal",
    "likeCount": "Likes",
    "likes_per_1kViews": "Beliebtheit (Likes pro 1000 Views)",
    "n_toplevel_user_comments": "Nutzerkommentare (ohne Replies)",
    "n_user_replies": "Nutzerreplies",
    "viewCount": "Views",
    "duration": "Videolänge",
    "mod_activity": "Moderationsaktivität",
    "responsivity": "Responsivität",
    "commentCount": "Kommentaranzahl",
    "replies_sentiment_mean": "Sentiment-Index (Replies)",
    "removed_comments_perc": "gelöschte Kommentare [%]",
    "comment_word_count": "Anzahl Kommentare (median)",
    "mean_word_count": "Mittlere Kommentarlänge",
    "comments_per_author": "Kommentare pro Autor",
    "videoCount": "Videoanzahl (gesamt)",
    "subscriberCount": "Abonennt*innen",
    "available_comments": "verfügb. Kommentare",
    "removed_comments": "gelöschte Kommentare",
    "comments_per_1kViews": "Kommentare pro 1000 Views",
    "categoryId": "YouTube Kategorie",
    "ratio_RepliesToplevel": "Reply Intensität (Reply vs Toplevel Kommentare)",
    "ZDF_content_references": "ZDFmediatheks Verweise",
    "references_per_video": "ZDFmediatheks Verweise pro Video",
    "toplevel_neutrality": "Neutralität",
    "gender_identified": "Anteil Autor*innen mit bekanntem Geschlecht",
}


# =============================================================================
# Plotly settings (include a button allowing select / deselect functionality)
# =============================================================================
px_select_deselect = dict(
    font=dict(size=18),
    updatemenus=[
        dict(
            type="buttons",
            direction="left",
            buttons=list(
                [
                    dict(
                        args=["visible", "legendonly"],
                        label="Deselect All",
                        method="restyle",
                    ),
                    dict(args=["visible", True], label="Select All", method="restyle"),
                ]
            ),
            pad={"r": 10, "t": 10},
            showactive=False,
            x=1,
            xanchor="right",
            y=1.1,
            yanchor="top",
        ),
    ],
)
