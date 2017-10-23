from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = (SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate())

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = (spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())
    
# Start running the query that prints the running counts to the console
query = (lines
    .writeStream
    .format("console")
    .start())

query.awaitTermination()
