from confluent_kafka import Consumer

consumer = Consumer(
    'content.trustpilot.reviews.orange',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8'),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

for msg in consumer:
    print(msg.value)
