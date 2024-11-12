import os

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = r"C:\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

# Import SparkSession from PySpark
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("SampleApp") \
    .getOrCreate()

# Sample data with different names
data = [("Ram", 28), ("Raja", 35), ("Kumar", 50)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()
