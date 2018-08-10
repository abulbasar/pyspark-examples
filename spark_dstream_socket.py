from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel

"""
Before running this script run "nc -lk 9999" 
command to open a server socket on port 9999

Before submitting the job, ensure the env variables are set properly.


export SPARK_HOME=/usr/lib/spark-2.2.0-bin-hadoop2.7
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
unset  PYSPARK_DRIVER_PYTHON_OPTS
#If you are using Spark 2.2+, you need JDK 1.8. So set JAVA_HOME that points to JDK1.8
export JAVA_HOME=/usr/java/jdk1.8.0_144/

Start the spark streaming application by running the following command
$SPARK_HOME/bin/spark-submit spark_dstream_socket.py

You may like to reduce the logging level to WARN in log4j.properties configuration
found in $SPARK_HOME/conf/log4j.properties. Copy this file from the template if not already present.


"""
batch_interval = 2
hostname, port = "localhost", 9999 

sc = SparkContext()
ssc = StreamingContext(sc, batch_interval)
lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_ONLY)

# Print the raw dstream
lines.pprint()

# Save the raw DStream
lines.saveAsTextFiles("raw/data", "csv")

# word_count = lines.flatMap(lambda line: line.split()) \
#            .map(lambda w: (w, 1)) \
#            .reduceByKey(lambda v1, v2: v1 + v2)
# word_count.pprint()

ssc.start()
ssc.awaitTermination()


