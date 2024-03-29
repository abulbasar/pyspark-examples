from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions

"""
kafka version: kafka_2.12-2.8.1
Spark version: spark-3.2.1-bin-hadoop3.2.tgz

export SPARK_HOME=/app/spark

$SPARK_HOME/bin/spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 structured_streaming_kafka.py

Note: this code has been successfully tested with 2.3.4-bin-hadoop2.7, but it does not work with spark 2.4.*
bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 structured_streaming_kafka.py

"""


def main():
    conf = SparkConf().setIfMissing("spark.master", "local[*]")

    spark = (SparkSession
             .builder
             .config(conf=conf)
             .appName("StructuredStreamingWithKafka")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    
    topic = "events"
    # Source: subscribe to 1 topic in Kafka
    raw = (spark
           .readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "demo1:9092,demo2:9092")
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger","100")
           .option("failOnDataLoss", "false")
           .load())
    
    # transformation
    enriched = (raw
               .withColumn("key", functions.expr("CAST(key AS STRING)"))
               .withColumn("value", functions.expr("CAST(value AS STRING)")))

    # sink
    (enriched
     .writeStream
     .format("console")
     .option("truncate", False)
     .option("numRows", 1000)
     .start())

    spark.streams.awaitAnyTermination()
    print("Process is complete")
    spark.stop()

if __name__ == "__main__":
    main()


