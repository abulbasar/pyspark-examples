from kafka import KafkaConsumer

# To consume messages
consumer = KafkaConsumer("tnx_json",
                         group_id="my_group",
                         bootstrap_servers=["localhost:9092"])

for msg in consumer:
    print(msg)

