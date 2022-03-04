from kafka import KafkaConsumer

# To consume messages
host = "demo1"
topic = "t1"
port = 9092
group_id = "g1"

consumer = KafkaConsumer(topic,
                         auto_offset_reset = "earliest",
                         group_id = group_id,
                         enable_auto_commit = True,
                         auto_commit_interval_ms = 1000,
                         max_poll_records = 1000, 
                         bootstrap_servers=[f"{host}:{port}"])

for msg in consumer:
    print(msg)

