import json
from kafka import KafkaConsumer
from collections import defaultdict

consumer = KafkaConsumer(
    "sales-events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="sales-analytics"
)

print("Consumer started - listening for sales events...")
regional_sales = defaultdict(float)
count = 0

for message in consumer:
    event = message.value
    regional_sales[event["region"]] += event["amount"]
    count += 1
    print(f"Received #{count}: {event['salesperson']} sold {event['product']} for ${event['amount']} in {event['region']}")
    if count >= 50:
        print("\n=== FINAL SUMMARY ===")
        for region, total in sorted(regional_sales.items()):
            print(f"{region}: ${total:.2f}")
        break
