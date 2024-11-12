import os
os.environ['SPARK_HOME'] = "C:\\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(appName="ExampleApp")

print("SparkContext initialized:", sc)

sc.stop()

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

print("SparkSession initialized:", spark)

spark.stop()
