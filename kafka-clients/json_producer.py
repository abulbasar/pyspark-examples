import io
import random
from time import time
from kafka import KafkaProducer
import json

"""
Install pip
$ curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
$ python get-pip.py
$ pip install kafka


"""


# To send messages synchronously
producer = KafkaProducer(bootstrap_servers = "localhost:9092")

# Kafka topic
topic = "tnx_json"

def get_record():
    return {"id": "123"
            , "merchant_id": "m123"
            , "customer_id": "c345"
            , "amount": 100.1
            , "category": "pos"
            , "timestamp": int(time())}


for i in range(10):
    record = get_record()
    producer.send(topic, json.dumps(record).encode("utf-8"))
    
producer.flush()

