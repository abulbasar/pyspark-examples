from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *;
from pyspark.storagelevel import StorageLevel

"""
This code has been tested with spark 2.3.1. spark-kafka-10. is not supported for python

$ spark-submit \
--master local[2] \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1  \
spark_streaming_kafka_direct.py 


"""




appName = "KafkaStreams"
config = SparkConf().setAppName(appName)

props = []
props.append(("spark.rememberDuration", "10"))
props.append(("spark.batchDuration", "10"))
props.append(("spark.eventLog.enabled", "true"))
props.append(("spark.streaming.timeout", "30"))
props.append(("spark.ui.enabled", "true"))

config = config.setAll(props)

sc = SparkContext(conf=config)
ssc = StreamingContext(sc, 5)

topics = ["t1"]
kafka_params = {
   "zookeeper.connect" : "localhost:5181/kafka"
 , "metadata.broker.list" : "localhost:9092"
 , "group.id" : "KafkaDirectStreamUsingSpark"}

raw = KafkaUtils.createDirectStream(ssc, topics, kafka_params)

"""
def show_partition_count(rdd):
  if rdd is not None:
    print(rdd.getNumPartitions())


raw.foreachRDD(show_partition_count)
"""

raw.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
