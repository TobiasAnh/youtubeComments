# YouTube comment and sentiment analysis
This repo enables to fetch YouTube channel, video and comment infos and associated metrics (e.g. likes, views, date of publication and more). The main functionality aims to fetch all available comments of a given channel. Afterwards, comment-specific features are estimated (e.g. median comment lengths, response time). Besides these simple metrics, each comment is analyzed for their sentiment using a NLP model 'GermanSentiBert'.

# Prerequisites

1) YouTube Data API key is required, follow instructions here: 
https://developers.google.com/youtube/v3/getting-started

2) Store API keys file repository/.env as follows ... <br>
API_KEY_1=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx <br>
API_KEY_2=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

3) Repository structure 
```
youtubeComments/       
    .env               <-- TODO(!): create file with entries API_KEY_1 and API_KEY_2
    src/
        __init__.py
        funcs.py       <-- contains various api requests, data transformations, relabelling dictionary, etc.
        ...
    fetch.py
    sentiment_analysis.py
    transform.py
    report.py
    ...

```
4) Setup and activate virtual environment and install dependencies 

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

# Workflow fetching and analyzing comments 
Execute files in the following order (further instructions and info can be found within these files). 
1) fetch.py
2) sentiment_analysis.py
3) transform.py
4) report.py (optional)
5) wordclouds.py (optional)

When executing fetch.py, data/ folder is genrated. Here, storage of various csv files containing channel, video and comment information 

```
youtubeComments/       
    data/
        interim/    <- each channel gets own subfolder
        processed/  <- combines all channels 
        reports/    <- final tables and plotly graphs        
    ...

```

