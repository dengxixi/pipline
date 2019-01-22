from pyspark.sql import *

spark = SparkSession.builder.master("local").config("spark.some.config.option", "some-value").getOrCreate()


read = spark.read.format('com.databricks.spark.avro').load('people_avro')

read.write.parquet('parquet')