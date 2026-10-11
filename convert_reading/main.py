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
        
        # Apply the mathematical conversions
        # P(psi) = P(kPa)/6.895
        # T(F) = T(C)*1.8+32
        if "pressure" in data:
            data["pressure"] = round(data["pressure"] / 6.895, 2)
        
        if "temperature" in data:
            data["temperature"] = round((data["temperature"] * 1.8) + 32, 2)

        # Convert back to JSON string and encode
        new_payload = json.dumps(data).encode("utf-8")

        # Publish the transformed data with the "processed" status for BigQuery
        future = publisher.publish(
            topic_path, 
            new_payload, 
            status="processed"
        )
        future.result()
        print(f"Converted and forwarded record: {data}")
        
        # Acknowledge the message so it leaves the convert-sub queue
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