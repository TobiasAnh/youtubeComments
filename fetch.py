#!/usr/bin/env python3
import os
import pandas as pd
from pathlib import Path
from src.api import ytChannel

# =============================================================================
# Channel Ids overview
# =============================================================================
channels = pd.read_csv(Path(os.getcwd()).joinpath("references", "channelIds.csv"))
print(channels)

# =============================================================================
# API fetch starts with channelId (manual input)
# Check quota limit here: # https://console.cloud.google.com/apis
# =============================================================================

# Single channel (api data fetch)
oChannel = ytChannel("UCWKEY-aEu7gcv5ayIpgrnvg")
oChannel.fetchVideoIds()
oChannel.fetchAndStoreVideoStatistics(load_existing=False)
oChannel.fetchComments(load_existing=False)

# Data concatination and estimation of addtional metrics
oChannel.concatAndSaveComments()
oChannel.applyGermanSentibert()

# Combines data from every channel found in /interim
# When combined, new comment metrics are estimated
# In the end, three tables are stored (as csv): channels, videos and comments
os.system("python transform.py")

# Reporting (reporting uses the tables created above)
os.system("python report.py")
os.system("python wordclouds.py")
