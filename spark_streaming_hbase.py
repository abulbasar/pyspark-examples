from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *;
from pyspark.storagelevel import StorageLevel
import happybase


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

server = "localhost"
table_name = "/tables/stocks"
table = happybase.Connection(server).table(table_name)

def bulk_insert(batch):
    table = happybase.Connection(server).table(table_name)
    for r in batch:
        tokens = r.split(",")
        key = tokens[0] + "-" + tokens[7]
        value = {"info:date": tokens[0]
                 ,"info:open": tokens[1]
                 ,"info:high": tokens[2]
                 ,"info:low": tokens[3]
                 ,"info:close": tokens[4]
                 ,"info:volume": tokens[5]
                 ,"info:adjclose": tokens[6]
                 ,"info:symbol": tokens[0]
                }
        # Look at jupyter console to see the print output
        print(key, value)
        table.put(key, value)

def saveRDD(rdd):
        rdd.foreachPartition(bulk_insert)

raw.map(lambda p:p[1]).foreachRDD(saveRDD)



ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate


#bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.3 ~/workspace/pyspark/simpleKafkaStream.py
