import os
import time
import pandas as pd
from germansentiment import SentimentModel # model details see below
from youtubeComments.setup import storage_path

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


# =============================================================================
# Detailed model info
# =============================================================================

# German Sentiment analysis
# !pip install germansentiment
# #https://huggingface.co/oliverguhr/german-sentiment-bert
# 
# Paper: https://aclanthology.org/2020.lrec-1.202.pdf
# 
# Data Collection: Training data has been gathered fron numerous sources. Data labeling is partly manually but mostly derived from 
# ratings such as Google PlayStore, Holiday reviews, etc. (e.g. 1-2 Stars counts as negative and 4-5 stars positive; 3 stars excluded).
# Data set also augumented by wikipedia sentences to increase number of neutral german sentences (assuming wikipedia text is neutral).
#
# Preprocessing: Removed all URLs, numbers, non-German characters. Replaced smiley and emojos with sentiment tags. 
# Resulting number of traning is shown in Table 1 (paper).
# 
# Train/Valid/Test split: 70% for training, 20 % for validation (hyperparameter optimi.) and 10 % for testing.
# Additionally, they created two versions of the data set (one unbalanced and one balanced data set). 
# Unbalanced data set: contains ALL 5.3 Million samples (positives: ~3.7 millions, neutrals: ~1.0 million, negative: ~ 0.6 million)
# Balanced data set: contains ~1.8 Million samples (positives/neutrals/negatives: ~0.6 million)
# 
# Two main models have been used: FastText and BERT with performance metrics of F1 Scores and confusion matrices
# Additionally, everything done on the two different data sets 
#
# =============================================================================
