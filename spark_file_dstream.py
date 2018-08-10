from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel

"""

Before submitting the spark job, create a directory /user/cloudera/input 
and download the following python file inside it that generates new files that contain random numbers.
https://raw.githubusercontent.com/abulbasar/pyspark-examples/master/random_file_generator.py

run this file inside /home/cloudera/input
$ cd /home/cloudera/input
$ python random_file_generator.py


Before submitting the job, ensure the env variables are set properly. Version may be different on your system.
export SPARK_HOME=/usr/lib/spark-2.2.0-bin-hadoop2.7
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
unset  PYSPARK_DRIVER_PYTHON_OPTS

#If you are using Spark 2.2+, you need JDK 1.8. So set JAVA_HOME that points to JDK1.8
export JAVA_HOME=/usr/java/jdk1.8.0_144/
Start the spark streaming application by running the following command
$SPARK_HOME/bin/spark-submit spark_stream_socket.py
You may like to reduce the logging level to WARN in log4j.properties configuration
found in $SPARK_HOME/conf/log4j.properties. Copy this file from the template if not already present.

Submit the spark job
$ $SPARK_HOME/bin/spark-submit spark_file_dstream.py

"""

input_path = "file:///home/cloudera/input"
batch_interval = 3

sc = SparkContext()
ssc = StreamingContext(sc, batch_interval)

# The random number generator create number between 0 and 1. 
# We want to find the numbers that are greater than 0.99 or less than 0.01 
def is_outlier(v):
        n = float(v)
        return (v, n > 0.99 or n < 0.01)

lines = ssc.textFileStream(input_path).map(is_outlier)

# Print the raw dstream
lines.pprint()

ssc.start()
ssc.awaitTermination()
                                    
                                    
                                    
