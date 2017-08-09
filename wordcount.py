from pyspark import SparkContext

sc = SparkContext()
rdd = sc.parallelize(["hello world", "world is beautiful"])
print(rdd.collect())