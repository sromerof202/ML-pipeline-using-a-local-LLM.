import json
import os
import time

import pandas as pd
import redis

r = redis.Redis(host="redis", port=6379, db=0)


def stream_data():
    print("--- Starting Tinder Traffic Simulation ---")
    if not os.path.exists("email.csv") or not os.path.isfile("email.csv"):
        print("Error: email.csv not found or is not a file.")
        return

    # Read CSV in chunks to handle memory efficiently
    for chunk in pd.read_csv("email.csv", chunksize=5):
        for index, row in chunk.iterrows():
            event = {
                "message_id": str(row.get("id", index)),
                "user_id": str(row.get("user", "unknown")),
                "content": str(row.get("content", "Hello world")),
                "timestamp": str(time.time()),
            }
            # Push to Redis
            r.rpush("ml_task_queue", json.dumps(event))
            print(f"[Ingestor] Queued msg from {event['user_id']}")

            # Simulate real-time gap
            time.sleep(1.0)


if __name__ == "__main__":
    time.sleep(5)  # Wait for Redis
    stream_data()
