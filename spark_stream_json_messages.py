from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import * 


"""
Before submitting the job, ensure the env variables are set properly.

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
unset  PYSPARK_DRIVER_PYTHON_OPTS

Start the spark streaming application by running the following command
$SPARK_HOME/bin/spark-submit spark_stream_json_messages.py

You may like to reduce the logging level to WARN in log4j.properties configuration
found in $SPARK_HOME/conf/log4j.properties. Copy this file from the template if not already present.


"""
hostname, port = "localhost", 9999
batch_interval = 2

sc = SparkContext()
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, batch_interval)
lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_ONLY)

# Print the raw dstream
#lines.pprint()

def top10_hashtags(tweets):
    tweets_terms = tweets.select(explode(split("text", ' ')).alias("term"))
    return (tweets_terms
     .filter("term like '#%'")
     .groupBy("term")
     .count()
     .orderBy(desc("count"))
    )

def convert_to_dataframe(rdd):
    if rdd.count() > 0:
        tweets = sqlContext.read.json(rdd)
        top10_hashtags(tweets).show(10, False)

lines.foreachRDD(convert_to_dataframe)

ssc.start()
ssc.awaitTermination()
