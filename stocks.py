from pyspark import SparkContext, SparkConf

input_path = "/user/cloudera/stocks"
output_path = "/user/cloudera/stocks-agg"

"""
Set JAVA_HOME to JDK1.8 in /etc/hadoop/conf/hadoop-env.sh and /etc/profile. For example,
export JAVA_HOME=/usr/java/jdk1.8.0_151
Note: you need to restart the YARN services or restart the VM box.

Submit script to execute
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 1 stocks.py

This code has been tested with Spark 2.2.3
"""


conf = SparkConf().setAppName("Stock")
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






