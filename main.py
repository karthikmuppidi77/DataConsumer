from kafka import KafkaConsumer
import redis
import json

r = redis.Redis(host='redis.finvedic.in', port=6379)

consumer = KafkaConsumer(
    'stock-topic',
    bootstrap_servers='your.kafka.server:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    data = message.value
    r.set(data['ticker'], json.dumps(data))
    print("Stored in Redis: ", data)
