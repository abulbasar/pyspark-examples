
#import sys
#from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import functions as F
import json

from pyspark.sql import SparkSession


"""
This code has been tested in CDH quick start VM 5.6.13 and python version Python 3.6.6 :: Anaconda, Inc.

Download the jar. Using packages commands seems to cause issues with net.jpountz.lz4 library.

$ wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8_2.11/2.3.2/spark-streaming-kafka-0-8_2.11-2.3.2.jar

Compatibility check
https://spark.apache.org/docs/latest/streaming-kafka-integration.html

Note, python API is not supported right now for Spark-kafka library.

Submit the streaming job
$ /usr/lib/spark-2.3.2-bin-hadoop2.7/bin/spark-submit --jars spark-streaming-kafka-0-8_2.11-2.3.2.jar  spark_stream.py

"""


zookeeper = "localhost:2181/kafka"
topic = "demo"

#sc = SparkContext(appName = "KafkaDStreamZK")

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)


raw_stream = KafkaUtils.createStream(ssc, zookeeper, "spark-streaming-consumer"
                    , {topic: 1}, storageLevel = StorageLevel.MEMORY_ONLY)

def convert_to_row(line):
    try:
        j = json.loads(line)
        row = Row(customer_id = j["customer_id"]
                , merchant_id = j["merchant_id"]
                , amount = j["amount"]
                , id = j["id"]
                , timestamp = j["timestamp"]
                , category = j["category"])
        return row
    except:
        pass

rows = raw_stream.map(lambda x: convert_to_row(x[1]))


def find_anomalies(partition):
    from cassandra.cluster import Cluster
    cluster = Cluster(["localhost"])
    cass = cluster.connect("cc")
    statement = "select id, amount_lower_threshold, amount_upper_threshold from customer where id = %s"

    records = []
    for row in partition:
        customer_id = row.customer_id
        rs = cass.execute(statement, [customer_id]).one()
        lower_threshold = rs.amount_lower_threshold
        upper_threshold = rs.amount_upper_threshold

        flag = 1.0 if row.amount > upper_threshold else 0.0
        enriched_row = Row(customer_id = row.customer_id
            , merchant_id = row.merchant_id
            , amount = row.amount
            , id = row.timestamp
            , timestamp = row.timestamp
            , category = row.category
            , flag_threshold = flag
            )
        records.append(enriched_row)
    return records

def save_and_show(rdd):
    if not rdd.isEmpty():

        # Store raw data into HDFS
        df = spark.createDataFrame(rdd)
        df.show(100, False)
        df.write.mode("append").saveAsTable("tnx_raw")

        # Store enriched data in cassandra
        rdd = rdd.mapPartitions(find_anomalies)
        df = spark.createDataFrame(rdd)

        (df
            .write
            .mode("append")
            .format("org.apache.spark.sql.cassandra")
            .options(table = "transactions", keyspace = "cc")
            .save())



rows.foreachRDD(save_and_show)


ssc.start()
ssc.awaitTermination()
