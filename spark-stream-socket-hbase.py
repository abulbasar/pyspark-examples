from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import happybase
from datetime import datetime

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
ssc = StreamingContext(sc, 2)
lines = ssc.socketTextStream("localhost", 9999)

# Print the raw dstream
lines.pprint()

# Save the raw DStream
lines.saveAsTextFiles("raw/data", "csv")


server = "localhost"
table_name = "/tables/stocks"
table = happybase.Connection(server).table(table_name)

def bulk_insert(batch):
    table = happybase.Connection(server).table(table_name)
    for value in batch:
        key = datetime.now().strftime('%s')
        table.put(key, {"info:raw": str(value)})

lines.foreachRDD(lambda rdd: rdd.foreachPartition(bulk_insert))



ssc.start()
ssc.awaitTermination()
