import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import StorageLevel

"""
This code has been tested in CDH quick start VM 5.6.13 and python version Python 3.6.6 :: Anaconda, Inc.

Download the jar. Using packages commands seems to cause issues with net.jpountz.lz4 library.
$ wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8_2.11/2.3.2/spark-streaming-kafka-0-8_2.11-2.3.2.jar

Compatibility check 
https://spark.apache.org/docs/latest/streaming-kafka-integration.html

Note, python API is not supported right now for Spark-kafka library.

Submit the streaming job
$ /usr/lib/spark-2.3.2-bin-hadoop2.7/bin/spark-submit --jars spark-streaming-kafka-0-8_2.11-2.3.2.jar  kafka_stream_zk_dstream.py

"""


zookeeper = "localhost:2181/kafka"
topic = "demo"

sc = SparkContext(appName = "PythonStreamingKafkaSource")
ssc = StreamingContext(sc, 10)


dstream = KafkaUtils.createStream(ssc, zookeeper, "spark-streaming-consumer", {topic: 1}, storageLevel = StorageLevel.MEMORY_ONLY)
lines = dstream.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()
