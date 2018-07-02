from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""
Start the file source: 
$ shuf -n $(($RANDOM % 10)) ~/tweets.small.json > /user/mapr/tweets_raw/$(date +%s).json

Above command randomly selects 10 lines from the ~/tweets.small.json and save them in a new file
under /user/mapr/tweets_raw/

The tweets.small.json can be found in 
https://raw.githubusercontent.com/abulbasar/data/master/tweets.small.json 

"""

# Create spark session
spark = (SparkSession
    .builder
    .appName("StructuredStreamingWithFileSourceMaprDBSink")
    .getOrCreate())

input_path = "file:///user/mapr/tweets_raw"

# Schema of the input messages
schema = StructType([
    StructField("id", LongType(), True),
    StructField("lang", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True),
    StructField("created_at", StringType(), True)
])

#File Source: json input data
raw = spark.readStream.format("json").schema(schema).load(input_path)

"""
Start the socket source
$ nc -nlk 9999
raw = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
"""

# Console Sink
(raw
    .writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .start())

spark.streams.awaitAnyTermination()

