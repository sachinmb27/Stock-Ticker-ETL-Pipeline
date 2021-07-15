import aiohttp
import asyncio
import time
import os
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future
import json

PROJECT_ID = 'egen-training-mbs'
TOPIC_ID = 'sample_topic'

class PublishToPubSub:
    def __init__(self):
        self.project_id = PROJECT_ID
        self.topic_id = TOPIC_ID
        self.publisher_client = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    # Asynchronously calls API to fetch data
    async def get_data(self, session, url):
        async with session.get(url) as resp:
            data = await resp.json()
            return data

    # Function that handles the API calls
    async def main(self, urls):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                tasks.append(asyncio.ensure_future(self.get_data(session, url)))

            original_data = await asyncio.gather(*tasks)
            for data in original_data:
                self.publish_message_to_topic(json.dumps(data, indent=2))
    
    # def get_callback(self, publish_future: Future, data: str) -> callable:
    #     def callback(publish_future):
    #         try:
    #             print(publish_future)
    #         except futures.TimeoutError:
    #             print(data)
        
    #     return callback
    
    # Publishes the message to Pub/Sub topic
    def publish_message_to_topic(self, message: str) -> None:
        # client returns a future when a message is published
        publish_future = self.publisher_client.publish(self.topic_path, message.encode('utf-8'))
        print(publish_future.result())