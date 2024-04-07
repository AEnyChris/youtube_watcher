import sys
import logging
import requests
from config import config
import json
from pprint import pformat
from ctypes import *
CDLL("C:/Users/Enyone Christian/Documents/DEB/youtube_watcher/venv/Lib/site-packages/confluent_kafka.libs/librdkafka-5d2e2910.dll")

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer




def fetch_playlist_items_page(google_api_key, playlist_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
            "key":google_api_key,
            "playlistId":playlist_id,
            "part":"contentDetails",
            "pageToken": page_token
            })

    payload = json.loads(response.text)
    logging.debug(f"GOT {payload}")
    return payload

def fetch_video_page(google_api_key, video_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", params={
            "key":google_api_key,
            "id":video_id,
            "part":"snippet, statistics",
            "pageToken": page_token
            })

    payload = json.loads(response.text)
    logging.debug(f"GOT {payload}")
    return payload


def fetch_playlist_items(google_api_key, playlist_id, page_token=None):
    payload = fetch_playlist_items_page(google_api_key, playlist_id, page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_playlist_items(google_api_key, playlist_id, next_page_token)


def fetch_videos(google_api_key, playlist_id, page_token=None):
    payload = fetch_video_page(google_api_key, playlist_id, page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_videos(google_api_key, playlist_id, next_page_token)


def summarize_video_details(video):

    return{
        "video_id": video["id"],
        "title":video["snippet"]["title"],
        "views":video["statistics"].get("viewCount"),
        "likes":video["statistics"].get("likeCount"),
        "comments":video["statistics"].get("commentCount")
    }
def on_delivery(err, record):
    pass


def main():
    logging.info("START")

    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_videos_value_schema = schema_registry_client.get_latest_version("yputube_videos-value")

    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client, 
            youtube_videos_value_schema.schema.schema_str,
            ),
    }

    producer = SerializingProducer(kafka_config)
    google_api_key = config["google_api_key"]
    playlist_id = config["playlist_id"]
    
    for video_item in fetch_playlist_items(google_api_key, playlist_id):
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(google_api_key, video_id):
            logging.info(f"GOT: {pformat(summarize_video_details(video))}")
            
            producer.produce(
                topic="yputube_videos",
                key=video_id,
                value={
                    "TITLE":video["snippet"]["title"],
                    "VIEWS":int(video["statistics"].get("viewCount", 0)),
                    "LIKES":int(video["statistics"].get("likeCount", 0)),
                    "COMMENTS":int(video["statistics"].get("commentCount", 0)),
                    },
                on_delivery=on_delivery
            )
    producer.flush()
  
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  
    sys.exit(main())