import os
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Functions for YouTube API requests
def setupYouTube():
    """
    Build YouTube instance (Version 3) with account-related API key.
    Requires API key stored in .env file

            Parameters:
                None.
            Return:
                youtube (googleapiclient.discovery.Resource): youtube request instance
    """

    # load .env entries as environment variables
    # NOTE: .env needs to be filled manually
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
    first_key = os.environ.get("API_KEY_1")
    second_key = os.environ.get("API_KEY_2")

    try:
        youtube = build(serviceName="youtube", version="v3", developerKey=first_key)
        channels_list_response = (
            youtube.videos().list(part="snippet", id="jNQXAC9IVRw").execute()
        )

        # Check if the request was successful
        if "items" in channels_list_response:
            print("Connection successful (API key 1)")
            return youtube

    except Exception as e:
        print(e)
        print("Trying API Key 2")

        try:
            youtube = build(
                serviceName="youtube", version="v3", developerKey=second_key
            )
            channels_list_response = (
                youtube.videos().list(part="snippet", id="jNQXAC9IVRw").execute()
            )

            # Check if the request was successful
            if "items" in channels_list_response:
                print("Connection successful (API key 2)")
                return youtube

        except Exception as e:
            print(e)
