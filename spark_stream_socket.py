from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Before running this script run "nc -lk 9999" 
# command to open a server socket on port 9999

sc = SparkContext()
ssc = StreamingContext(sc, 2)
lines = ssc.socketTextStream("localhost", 9999)
# words = lines.flatMap(lambda line: line.split()) \
#            .map(lambda w: (w, 1)) \
#            .reduceByKey(lambda v1, v2: v1 + v2)
# words.pprint()
lines.pprint()

ssc.start()
ssc.awaitTermination()
