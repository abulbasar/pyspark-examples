import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext

sc = SparkContext(appName="PythonStreamingKafka")
ssc = StreamingContext(sc, 3)

zkQuorum, topic = 'localhost:9092', 'test'
kvs = KafkaUtils.createStream(ssc, zkQuorum,"my_group", {topic: 3})

lines = kvs.map(lambda x: x.value)
counts = (lines.flatMap(lambda line: line.split(" "))
  .map(lambda word: (word, 1)) 
  .reduceByKey(lambda a, b: a+b)
 )

 counts.pprint()

 ssc.start()
 ssc.awaitTermination()