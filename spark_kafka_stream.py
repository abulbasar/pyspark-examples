from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

batch_interval = 3
sc = SparkContext(appName="PythonStreamingKafka")
ssc = StreamingContext(sc, batch_interval)

zookeeper, topic = 'localhost:2181/kafka', 'intel'
raw = KafkaUtils.createStream(ssc, zookeeper,"spark_cg", {topic: 3})
#raw.pprint()

lines = raw.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()
print("Spark streaming application has been terminated")
