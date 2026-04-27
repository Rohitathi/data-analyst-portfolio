import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

regions = ["North", "South", "East", "West"]
salespersons = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

print("Starting producer - sending sales events...")
for i in range(50):
    event = {
        "event_id": i,
        "timestamp": datetime.now().isoformat(),
        "salesperson": random.choice(salespersons),
        "region": random.choice(regions),
        "amount": round(random.uniform(100, 5000), 2),
        "product": random.choice(["Software", "Hardware", "Services"])
    }
    producer.send("sales-events", value=event)
    print(f"Sent: {event}")
    time.sleep(0.5)

producer.flush()
print("Done sending 50 events!")
