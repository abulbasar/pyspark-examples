from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

"""
Submit the application 
$ spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
--verbose spark_kafka_dstream.py
"""


# Spark streaming batch interval 
batch_interval = 3

# Build spark context
sc = SparkContext(appName="PythonStreamingKafka")

# Build spark streaming context
ssc = StreamingContext(sc, batch_interval)

zookeeper, topic = 'localhost:2181/kafka', 'demo'

# Build Dstream associated with Kafka
kafkaParams = {"auto.offset.reset": "smallest"}
raw = KafkaUtils.createStream(ssc, zookeeper,"spark_cg",{topic: 3}, 
                              kafkaParams = kafkaParams, storageLevel = StorageLevel.MEMORY_ONLY)
#raw.pprint()

# Kafka DStream contains RDD of k, v pair - tuple. The message is stored in the value. 
lines = raw.map(lambda x: x[1])

# Print the values
lines.pprint()

# Star listening with spark streaming context 
ssc.start()

# Keep the spark driver program running and wait for manual termination. 
ssc.awaitTermination()
print("Spark streaming application has been terminated")
