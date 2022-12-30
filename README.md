# YouTube comment and sentiment analysis
This repo enables to fetch YouTube channel, video and comment infos and associated metrics (e.g. likes, views, date of publication and more). The main functionality aims to fetch all available comments of a given channel. Afterwards, comment-specific features are estimated (e.g. median comment lengths, response time). Besides these simple metrics, each comment is analyzed for their sentiment using a NLP model 'GermanSentiBert'.

# Prerequisites

1) YouTube Data API key is required, follow instructions here: 
https://developers.google.com/youtube/v3/getting-started

2) Local folder structure 
```
youtubeComments/       <-- pulled repo (set absolute local path as 'project_path' in .env file)
    .env               <-- create file with entries API_KEY_1, API_KEY_2 and project_path
    data/              <-- folder and subfolders generated when data.py executes 
        interim/     
        processed/   
        reports/
    references/         
        channelIds.csv <-- relevant channelIds can be found here 
        ...                    
    src/
        __init__.py
        data.py        <-- contains various api requests, data transformations, relabelling dictionary, etc.
        ...
    fetch.py
    sentiment_analysis.py
    transform.py
    report.py
    ...

```

3) Python libraries
```
pip install -r requirements.txt
```
# Fetching and analyzing comments 
Execute files in the following order (further instructions and info can be found within these files)
1) fetch.py
2) sentiment_analysis.py
3) transform.py
4) report.py (optional)
5) wordclouds.py (optional)

