import matplotlib.pyplot as plt
import nltk
import pandas as pd
from wordcloud import WordCloud

from src.data import project_path, storage_path, reports_path


# Gathering and defining stopwords prior to wordcloud creation 
# Stopwords (common words with no/little meaning)
nltk.download('stopwords', quiet=True)
stopwords_nltk = nltk.corpus.stopwords.words('german')
stopwords_foundOnline = list(pd.read_csv(project_path.joinpath("references", "stopwords_de.csv")).columns)
own_stopwords = ["bzw", "etc", "einfach", "schon", "sieht", "halt", "genau", "ne", "eigentlich", "eher", "finde", "sagen", "sogar"]
stopwords_combined = (stopwords_foundOnline + stopwords_nltk + own_stopwords)

channel_paths = [x for x in storage_path.iterdir() if x.is_dir()]

# Start generating wordclouds (loops through channel paths)
for channel_path in channel_paths[2:]:

    selected_comments_df = pd.read_csv(channel_path.joinpath("all_comments_withSentiment.csv"), 
                                       index_col=0,
                                       lineterminator="\r",
                                       parse_dates=["publishedAt", "comment_published"],
                                       dtype={"video_published_year": int})
    
    channel_title = selected_comments_df["videoOwnerChannelTitle"][0]
    channel_foldername = channel_title.replace(" ", "_").replace("&", "_")
    
    # Generate word clouds for every year (within subfolder)
    channel_years = list(selected_comments_df["video_published_year"].unique())
    reports_path.joinpath(channel_foldername).mkdir(parents=True, exist_ok=True)
    
    for year in channel_years:
        
        selected_comments_filtered = selected_comments_df.query("comments_published_year == @year")
        comments_for_wordcloud_filtered = selected_comments_filtered["comment_string"]
        comments_bagOfWords = ' '.join(comments_for_wordcloud_filtered.apply(str))
        
        wordcloud_filtered = WordCloud(stopwords = stopwords_combined, 
                                        background_color="white", 
                                        margin = 10, 
                                        width = 1024, height = 768).generate(comments_bagOfWords)
        
        if year == 2022:
            last_comment = selected_comments_filtered["comment_published"].max().date()
            time_window = f'{year} (bis {last_comment.day-1}.{last_comment.month}.)'
        else:
            time_window = year
        
        plt.imshow(wordcloud_filtered, interpolation='bilinear')
        plt.suptitle(f'"{channel_title}" WordCloud')
        plt.title(f'YT-Kommentare von {time_window}')
        plt.axis("off")
        plt.savefig(reports_path.joinpath(channel_foldername, f'WordCloud_{channel_foldername}_{year}.png'),
                    dpi = 600)
        
        print(f'{channel_title} | Wordcloud done for {year}')
