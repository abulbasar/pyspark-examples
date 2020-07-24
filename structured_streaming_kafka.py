from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions

"""

export SPARK_HOME=/usr/lib/spark-2.2.0-bin-hadoop2.7
bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 structured_streaming_kafka.py

kafka version: kafka_2.11-2.4.1

Note: this code has been successfully tested with 2.3.4-bin-hadoop2.7, but it does not work with spark 2.4.*

"""


def main():
    conf = SparkConf().setMaster("local[*]")

    spark = (SparkSession
             .builder
             .config(conf=conf)
             .appName("StructuredStreamingWithKafka")
             .getOrCreate())

    topic = "demo"
    # Source: subscribe to 1 topic in Kafka
    raw = (spark
           .readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger","100")
           .option("failOnDataLoss", "false")
           .load())
    
    # transformation
    enriched = raw.selectExpr("*", "CAST(key AS STRING)", "CAST(value AS STRING)")

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


