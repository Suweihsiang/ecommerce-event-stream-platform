from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime,timezone

producer = KafkaProducer(
        bootstrap_servers="kafka.kafka.svc.cluster.local:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
event_types = ["view", "click", "add_to_cart", "purchase"]
categories = ["book", "toy", "3c", "fashion", "beauty"]
devices = ["ios", "android", "web"]

while True:
    is_bad = random.random() < 0.2 # 要不要送壞掉的資料進去
    is_duplicate = random.random() < 0.2 # 要不要重複送資料進去

    data = {
        "user_id": f"u_{random.randint(1, 100)}",
        "event_type": ("INVALID_EVENT" if is_bad and random.random() < 0.33 else random.choice(event_types)),
        "product_id": f"p_{random.randint(1, 1000)}",
        "category": random.choice(categories),
        "price": (-100 if is_bad and random.random() < 0.33 else random.randint(100, 3000)),
        "event_time": (None if is_bad and random.random() < 0.33 else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
        "device": random.choice(devices),
    }
    try:
        producer.send("test-topic",data)
        if is_duplicate:
            producer.send("test-topic",data)
            print("[DUPLICATE SENT]")
        producer.flush()
        print(f"[SENT] {data}")
    except Exception as e:
        print(f"some error occurred: {e}")
    time.sleep(5)