from pyspark.sql import *
from pyspark.sql import types as T

from pyspark import SparkContext


def parse(line):
    items = line.split(",")
    return (int(items[0]), items[1], int(items[2]))

sc = SparkContext()


spark = SparkSession.builder.master("local").config("spark.some.config.option", "some-value").getOrCreate()


schema = T.StructType([
    T.StructField("class", T.IntegerType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("score", T.IntegerType(), True),
    ]
)

rdd = sc.textFile('/Users/zdeng-ext/school/test.csv').map(parse)
df = spark.createDataFrame(rdd,schema)



df.write.format('com.databricks.spark.avro').save("people_avro")