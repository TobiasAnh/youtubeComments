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
    references/         
        channelIds.csv <-- relevant channelIds and other infos can be found here 
        ...                    
    src/
        __init__.py
        funcs.py 
        api.py      
        ...
    fetch.py
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
Exemplary workflow demonstrated in fetch.py 


```
youtubeComments/       
    data/
        interim/    <- each channel gets own subfolder
        processed/  <- combines all channels 
        reports/    <- final tables and plotly graphs        
    ...

```

