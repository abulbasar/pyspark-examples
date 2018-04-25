from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

"""

Start a socket streaming source using the following command
$ nc -nlk 9999 

Above 9999 is the port number on which socket messages will be published.
Set the logging level to WARN before you launch spark streaming service so that 
streaming messages are not lost in the logs message.  

Open $SPARK_HOME/conf/log4j.properties and set the following
log4j.rootCategory=WARN, console


Then start the spark streaming application
$SPARK_HOME/bin/spark-submit structured-streaming.py
"""
spark = (SparkSession
    .builder
    .appName("StructuredStreaming")
    .getOrCreate())

host, port = "localhost", 9999

# Source: Create a streaming dataFrame representing the stream of input lines from connection to localhost:9999
lines = (spark
    .readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load())

print("lines isStreaming: ", lines.isStreaming)

# Sink: Start running the query that prints the running counts to the console
query = (lines
    .writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 1000)
    .start())

spark.streams.awaitAnyTermination()

spark.close()
