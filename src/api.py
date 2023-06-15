import os
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build


def testAndGenerateInstance(developerKey: str):
    """
    Execute a test API request and return instance when succesful

    Parameter:
        developerKey: valid API key
    Returns:
        youtube request instance
    """
    youtube = build(serviceName="youtube", version="v3", developerKey=developerKey)
    testVideoId = "jNQXAC9IVRw"  # oldest YT video (just for testing purposes)
    channels_list_response = (
        youtube.videos().list(part="snippet", id=testVideoId).execute()
    )
    if "items" in channels_list_response:
        print("API key functional!")
        return youtube


# Functions for YouTube API requests
def setupYouTube():
    """
    Test and returns a YouTube instance (Version 3) by trying two API keys.
    Requires API keys stored in .env file in repository

    Return:
        youtube (googleapiclient.discovery.Resource): youtube request instance
    """

    # load .env entries as environment variables
    # NOTE: .env needs to be filled manually
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
    first_key = os.environ.get("API_KEY_1")
    second_key = os.environ.get("API_KEY_2")

    # Execute a API request for testing
    try:
        return testAndGenerateInstance(first_key)

    # If first API requests fails, switch to second API key
    except Exception as e:
        print(e)
        print("Trying another API Key")

        return testAndGenerateInstance(second_key)
