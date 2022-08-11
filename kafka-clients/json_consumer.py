from kafka import KafkaConsumer
  
"""
Create a topic in Kafka
$ /app/kafka/bin/kafka-topics.sh --bootstrap-server sa01:9092,sa02:9092 \
--topic events --create --replication-factor 1 --partitions 2

Start the consume
python3 json_consumer.py

"""


# To consume messages

host = "sa01"
topic = "events"
port = 9092
group_id = "g1"

# Kafka console consumer properties
# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
  
consumer = KafkaConsumer(topic,
                         auto_offset_reset = "earliest",
                         group_id = group_id,
                         enable_auto_commit = True,
                         auto_commit_interval_ms = 1000,
                         max_poll_records = 1000,
                         bootstrap_servers=[f"{host}:{port}"])

for msg in consumer:
    print(msg)
