
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import *

"""
Launch spark application
$ /usr/lib/spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1,org.xerial.snappy:snappy-java:1.1.4 \
structured_streaming_kafka_aggregate.py

"""
spark = (SparkSession
    .builder
    .appName("StructuredStreamingWithKafka")
    .getOrCreate())

# Source: subscribe to 1 topic in Kafka
raw = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "sandbox-hdp:6667")
  .option("subscribe", "demo")
  .load())
  

enriched = (raw
.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
.selectExpr("to_date(get_json_object(value, '$.timestamp')) as time"
            , "get_json_object(value, '$.amount') as amount"
            , "get_json_object(value, '$.customer_id') as customer_id")
.groupBy("customer_id")
.agg(expr("avg(cast(amount as float))").alias("avg_amount"))
)
# Sink: defined console sink
query = (enriched
    .writeStream
    .outputMode("complete")
    .trigger(processingTime='5 seconds')
    .format("console")
    .option("truncate", False)
    .option("numRows", 1000)
    .start())

spark.streams.awaitAnyTermination()


