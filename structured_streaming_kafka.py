
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

"""
Launch spark application
$ /usr/lib/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.xerial.snappy:snappy-java:1.1.4 \
structured_streaming_kafka.py

"""
spark = (SparkSession
    .builder
    .appName("StructuredStreamingWithKafka")
    .getOrCreate())

# Source: subscribe to 1 topic in Kafka
raw = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "demo")
  .load())
  
# Sink: defined console sink
query = (raw
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 1000)
    .start())

spark.streams.awaitAnyTermination()


