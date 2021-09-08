from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions

"""

$ spark-submit --master yarn --num-executors 1 data-conversion.py

"""


def main():
    
    input_path = "s3://data.einext.com/Map__Crime_Incidents_-_from_1_Jan_2003.csv.gz"
    output_path = "s3://tmp.einext.com/sfpd.parquet"

    conf = SparkConf().setMaster("yarn")

    spark = (SparkSession
             .builder
             .config(conf=conf)
             .appName("FileConversionApp")
             .getOrCreate())

    sc = spark.sparkContext
    print(spark)
    print(sc.uiWebUrl)
    sfpd = sfpd = spark.read.format("csv").options(header = True
        , inferSchema = True).load(input_path)
    sfpd.write.format("parquet").mode("overwrite").save(output_path)
    print("Conversion is complete")


if __name__ == "__main__":
    main()

