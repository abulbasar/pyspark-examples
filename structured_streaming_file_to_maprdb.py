
"""
This code has been tested in mapr sandbox 6.0.1
For detailed instruction, following this article
https://blog.einext.com/hadoop/maprstream-to-maprdb
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""
Data source:
The tweets.small.json can be found in 
https://raw.githubusercontent.com/abulbasar/data/master/tweets.small.json

Download the datasource:
$ wget https://raw.githubusercontent.com/abulbasar/data/master/tweets.small.json 

Start the file source: 
$ shuf -n $(($RANDOM % 10)) ~/tweets.small.json > /user/mapr/tweets_raw/$(date +%s).json
Above command randomly selects 10 lines from the ~/tweets.small.json and save them in a new file
under /user/mapr/tweets_raw/

Launch the streaming job
$ /opt/mapr/spark/spark-2.2.1/bin/spark-submit --verbose ~/structured_streaming_file_to_maprdb.py
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

# Transformation
transformed = (df#.orderBy("id")
          .withColumn("id", expr("cast(id as string)"))
          .withColumnRenamed("id", "_id"))

"""
Start the socket source
$ nc -nlk 9999
raw = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
"""
          
          
checkpoint_path = "/user/mapr/checkpoint"
maprdb_table = "/tables/tweets"
 
# Send the stream to MaprDB sink
(transformed
  .writeStream
  .outputMode("append")
  .format("com.mapr.db.spark.streaming")
  .option("checkpointLocation", checkpoint_dir)
  .option("tablePath", maprdb_table)
  #.option("idFieldPath", "_id")
  #.option("createTable", True)
  .option("bulkMode", True)
  .option("sampleSize", 1000)
).start()

# Wait for any sink to terminate to end the program
spark.streams.awaitAnyTermination()

