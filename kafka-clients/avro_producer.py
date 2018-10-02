import io
import random
import avro.schema
from avro.io import DatumWriter
from kafka import SimpleProducer, KafkaProducer
from kafka import KafkaClient
from time import time



# To send messages synchronously
producer = KafkaProducer(bootstrap_servers = "localhost:9092", compression_type = "gzip")

# Kafka topic
topic = "tnx"

# Path to user.avsc avro schema
schema_path = "/home/cloudera/workspace/kafka-clients-python/transactions.avsc"
schema = avro.schema.Parse(open(schema_path).read())
print("Schema", schema.to_json())

writer = DatumWriter(schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)

def get_record():
    return {"id": "123"
            , "merchant_id": "m123"
            , "customer_id": "c345"
            , "amount": 100.1
            , "category": "pos"
            , "timestamp": int(time())}


for i in range(10):
    record = get_record()
    writer.write(record, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send(topic, raw_bytes)
producer.flush()

