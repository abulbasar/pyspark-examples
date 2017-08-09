from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *;
from pyspark.storagelevel import StorageLevel

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
 , "group.id" : "Kafka_MapR-Streams_to_HBase"}

raw = KafkaUtils.createDirectStream(ssc, topics, kafka_params)
raw.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
