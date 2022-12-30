import os
import time
import pandas as pd
from germansentiment import SentimentModel # model details see below
from src.data import storage_path

# Manually channel_path for sentiment analysis
channel_paths = [x for x in storage_path.iterdir() if x.is_dir()]
channel_path = channel_paths[-4]

sentiment = SentimentModel()
sentiment.predict_sentiment(["gar nicht schlecht"], True)

#TODO 
#implement probabilities in final dataframe

def sentiment_analysis(channel_path):
    """
    Executes sentiment analysis using the model "germansentiment".

    Parameters:
            channel_path (list): local folder for importing required .csv files
    Returns:
            No returns. Sentiment is appended to DataFrame which is stored again as csv file
            in channel_path
    """
    # Import all_comments_noSentiment.csv from channel folders 
    comments_for_sentiment = pd.read_csv(channel_path.joinpath("all_comments_noSentiment.csv"),
                                         index_col = 0, lineterminator="\r",
                                         parse_dates=["publishedAt", "comment_published"],
                                         dtype={"comment_string":"str"})
    
    channel_title = comments_for_sentiment["videoOwnerChannelTitle"][0]
    # Loop for sentiment analysis
    n_comments = len(comments_for_sentiment) # required for loop reporting
    sentiment_estimate = [] # sentiments go here
   
    print(f'Analyzing {n_comments} comments fetched in {channel_title} ...')
    start = time.time()
    for comment in comments_for_sentiment["comment_string"]:
        
        sentiment_estimate.append(sentiment.predict_sentiment([str(comment)]))
        fraction_done = len(sentiment_estimate) / n_comments
        
        # loop reporting (in per mil steps)
        if (fraction_done) > 0.001:
        
            time_per_pm = time.time()
            time_passed = time_per_pm - start
            print(f'{channel_title} | {round(fraction_done, 3)} done | time per loop: {round(time_passed, 2)}')
            
            start = time.time()
    
        
    # Augment sentiment to original data frame and store as csv
    comments_for_sentiment["sentiment"] = sentiment_estimate
    comments_for_sentiment.to_csv(channel_path.joinpath("all_comments_withSentiment.csv"), 
                                  lineterminator="\r")
    
    # if new csv file exists, delete old one
    if "all_comments_withSentiment.csv" in os.listdir(channel_path):
        file = channel_path.joinpath("all_comments_noSentiment.csv")
        os.remove(file)
        print("original csv deleted.")

sentiment_analysis(channel_path)