

from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel

input_path = "/user/mapr/stocks"
output_path = "/user/mapr/stocks-agg"

"""
Submit script to execute
/opt/mapr/spark/spark-2.3.1/bin/spark-submit stocks.py  --master yarn --driver-memory 1GB --num-executors 1
"'"


conf = SparkConf().setIfMissing("spark.master", "local").setAppName("Stock")
sc = SparkContext(conf = conf)

rdd = sc.textFile(input_path)

agg = (rdd
.filter(lambda line: not line.startswith("date"))
.map(lambda line: line.split(","))
.map(lambda tokens: (tokens[7], float(tokens[5])))
.groupByKey()
.mapValues(lambda values: sum(values)/len(values))
)
agg.saveAsTextFile(output_path)                              

