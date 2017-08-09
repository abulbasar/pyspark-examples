from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
Before running this script run "nc -lk 9999" 
command to open a server socket on port 9999

Start the spark streaming application by running the following command
$SPARK_HOME/bin/spark-submit spark_stream_socket.py

You may like to reduce the logging level to WARN in log4j.properties configuration
found in $SPARK_HOME/conf/log4j.properties. Copy this file from the template if not already present.


"""
sc = SparkContext()
ssc = StreamingContext(sc, 2)
lines = ssc.socketTextStream("localhost", 9999)
lines.pprint()

# word_count = lines.flatMap(lambda line: line.split()) \
#            .map(lambda w: (w, 1)) \
#            .reduceByKey(lambda v1, v2: v1 + v2)
# word_count.pprint()

ssc.start()
ssc.awaitTermination()


