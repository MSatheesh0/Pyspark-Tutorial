import os

os.environ['SPARK_HOME'] = "C:\\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("WordCountExample").getOrCreate()

lines_rdd = spark.sparkContext.textFile("./data/sample.txt")

word_counts_rdd = lines_rdd.flatMap(lambda line: line.split()) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

top_5_rdd = word_counts_rdd.take(5)
print("Top 5 frequent words from RDD:", top_5_rdd)

df = spark.read.text("./data/sample.txt")

word_counts_df = df.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count().orderBy(col("count").desc())

top_5_df = word_counts_df.take(5)
print("Top 5 frequent words from DataFrame:", top_5_df)
