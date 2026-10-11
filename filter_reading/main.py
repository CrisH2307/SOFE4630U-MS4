import os
import json
from google.cloud import pubsub_v1

project_id = os.environ.get("GCP_PROJECT", "project-4cfdac74-a7d9-4d41-abf")
topic_name = "smart-meter-topic"
subscription_name = "filter-sub"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, subscription_name)
topic_path = publisher.topic_path(project_id, topic_name)

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        if data.get("pressure") is None or data.get("temperature") is None:
            print(f"Dropped invalid record: {data}")
            message.ack()
            return

        future = publisher.publish(topic_path, message.data, status="filtered")
        future.result()
        print(f"Forwarded valid record: {data}")
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.ack()

print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()