# YouTube comment and sentiment analysis
This repo enables to fetch YouTube channel, video and comment infos and associated metrics (e.g. likes, views, date of publication and more). The main functionality aims to fetch all available comments of a given channel. Afterwards, comment-specific features are estimated (e.g. median comment lengths, response time). Besides these simple metrics, each comment is analyzed for their sentiment using a NLP model 'GermanSentiBert'.

# Prerequisites

1) YouTube Data API key is required, follow instructions here: 
https://developers.google.com/youtube/v3/getting-started


2) Local folder structure 
```
repository/          <-- pulled repo (input absolute local path as project_path in setup.py)
    API_KEY.json     <-- create this json as follows {"first_key": "YOUR_API_KEY"}
    youtubeComments/
        setup.py
        ...
    data_fetch.py
    sentiment_analysis.py
    feature_engineering.py
    report.py
    ...
storage_path/        <-- created automatically (location for csv files with fetched channels, videos and comment data)
reports/             <-- created automatically (for final csv and plotly-html)
```

1) Python libraries
```
pip install -r requirements.txt
```
# Fetching and analyzing comments 
Execute files in the following order (further instructions and info can be found within these files)
1) data_fetch.py
2) sentiment_analysis.py
3) feature_engineering.py
4) report.py
5) data_update.py (not finished)

