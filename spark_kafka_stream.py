import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext

sc = SparkContext(appName="PythonStreamingKafka")
ssc = StreamingContext(sc, 3)

zkQuorum, topic = 'localhost:9092', 'test'
raw = KafkaUtils.createStream(ssc, zkQuorum,"my_group", {topic: 3})

lines = raw.map(lambda x: x.value)
lines.pprint()

 ssc.start()
 ssc.awaitTermination()
