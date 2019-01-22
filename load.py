from pyspark import SparkContext
from pyspark.sql import *
import psycopg2
import re
import pandas
spark = SparkSession.builder.master("local").config("spark.some.config.option", "some-value").getOrCreate()

conn = psycopg2.connect(database="school", user="peter", password="53a98wo", host="127.0.0.1", port="5432")
cur = conn.cursor()
cur.execute("CREATE TABLE school(class smallint, name varchar PRIMARY KEY, score smallint);")


student = spark.read.parquet('parquet')
list = student.collect()
#print list
for item in list:
    r = re.findall(r"class=(.*?), name=u'(.*?)', score=(.*?)\)",str(item))
    cur.execute("INSERT INTO school (class,name,score) VALUES (%s, %s, %s)", (r[0][0], r[0][1], r[0][2]))


df = student.groupBy('class')
list_group = df.avg('score').collect()
#print list_group
cur.execute("CREATE TABLE avg(class smallint , avg_score real);")
for item in list_group:
    r = re.findall(r"\d+\.?\d*",str(item))
    cur.execute("INSERT INTO avg(class,avg_score) VALUES (%s , %s)",(r[0],r[1]))

conn.commit()
