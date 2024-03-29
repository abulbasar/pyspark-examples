import io
import random
from time import time, sleep
from kafka import KafkaProducer
import json
from random import gauss, uniform, randint, choice
from uuid import uuid1

"""
Install pip
$ curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
$ python get-pip.py

Install kafka package
$ pip install kafka-python

Run the application
$ python json_producer.py

$ $KAFKA_HOME/bin/kafka-topics.sh  --bootstrap-server sa01:9092 --topic events --create --replication-factor 1 --partitions 1

Producer Properties
https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

"""

host = "sa01"
topic = "events"
port = 9092

# To send messages synchronously
producer = KafkaProducer(bootstrap_servers = f"{host}:{port}")

def get_record():
    return {"id": str(uuid1())
            , "merchant_id": "m" + str(randint(0, 1000))
            , "customer_id": "c" + str(randint(0, 1000000))
            , "amount": gauss(100.0, 25.0)
            , "category": choice(["pos", "net", "app"])
            , "timestamp": int(time())}

n = 100

for i in range(n):
    record = get_record()
    print(record)
    producer.send(topic, key = None, value = json.dumps(record).encode("utf-8"))
    sleep(0.3)

producer.flush()

