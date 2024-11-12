import os

os.environ['SPARK_HOME'] = "C:\\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Operations Example").getOrCreate()

numbers = [10, 15, 20, 25, 30]
rdd = spark.sparkContext.parallelize(numbers)

all_elements = rdd.collect()
print("All elements in the RDD:", all_elements)

doubled_rdd = rdd.map(lambda x: x * 2)
print("Doubled elements:", doubled_rdd.collect())

data = [("Satheesh", 45), ("Ram", 22), ("Raja", 34), ("Kumar", 29)]
rdd = spark.sparkContext.parallelize(data)
print("All elements of the RDD:", rdd.collect())

filtered_rdd = rdd.filter(lambda x: x[1] > 30)
print("Filtered RDD (age > 30):", filtered_rdd.collect())

count_filtered = filtered_rdd.count()
print("Number of elements with age > 30:", count_filtered)


filtered_rdd.foreach(lambda x: print(x))
