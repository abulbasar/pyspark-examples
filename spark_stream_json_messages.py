from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel


"""
Before running this script run "nc -lk 9999" 
command to open a server socket on port 9999

Before submitting the job, ensure the env variables are set properly.

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
unset  PYSPARK_DRIVER_PYTHON_OPTS

Start the spark streaming application by running the following command
$SPARK_HOME/bin/spark-submit spark_stream_socket.py

You may like to reduce the logging level to WARN in log4j.properties configuration
found in $SPARK_HOME/conf/log4j.properties. Copy this file from the template if not already present.


"""
sc = SparkContext()
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 2)
lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)

# Print the raw dstream
#lines.pprint()

def convert_to_dataframe(rdd):
    if rdd.count() > 0:
        sqlContext.read.json(rdd).show()


lines.foreachRDD(convert_to_dataframe)

# Save the raw DStream
lines.saveAsTextFiles("raw/data", "csv")

# word_count = lines.flatMap(lambda line: line.split()) \
#            .map(lambda w: (w, 1)) \
#            .reduceByKey(lambda v1, v2: v1 + v2)
# word_count.pprint()

ssc.start()
ssc.awaitTermination()
