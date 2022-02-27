from kafka import KafkaConsumer

# To consume messages
host = "demo1"
topic = "t1"
port = 9092

consumer = KafkaConsumer(topic,
                         group_id="my_group",
                         bootstrap_servers=[f"{host}:{port}"])

for msg in consumer:
    print(msg)

