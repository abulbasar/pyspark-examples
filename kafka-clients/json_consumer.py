from kafka import KafkaConsumer
  
"""
Create a topic in Kafka
$ /app/kafka/bin/kafka-topics.sh --bootstrap-server demo1:9092,demo2:9092 \
--topic events --create --replication-factor 1 --partitions 2

"""


# To consume messages

host = "demo1"
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
