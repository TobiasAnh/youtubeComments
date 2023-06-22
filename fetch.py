#!/usr/bin/env python3
import os
import pandas as pd
from pathlib import Path
from src.funcs import ytChannel

# =============================================================================
# Channel Ids overview
# =============================================================================
channels = pd.read_csv(Path(os.getcwd()).joinpath("references", "channelIds.csv"))
print(channels)

# =============================================================================
# API fetch starts with channelId (manual input)
# Check quota limit here: # https://console.cloud.google.com/apis
# =============================================================================

# Single channel
oChannel = ytChannel("UCjCbe-qZ4SW10SkXdPm7hPQ")
oChannel.getVideoIds()
oChannel.getAndSaveVideoStatistics(False)
oChannel.getComments(False)
oChannel.concatAndSaveComments()
oChannel.applyGermanSentibert()

os.system("python transform.py")
os.system("python report.py")
os.system("python wordclouds.py")

# Loop through all channels
# channelIds = channels["channelId"]

# for channelId in channelIds:
#     channel = ytChannel(channelId)
#     channel.getVideoIds()
#     channel.getAndSaveVideoStatistics(False)
#     channel.getComments(False)
#     channel.concatAndSaveComments()
#     channel.applyGermanSentibert()
