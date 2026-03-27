import os
os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .master("local[2]") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

print("SUCCESS - Spark version:", spark.version)
spark.stop()
print("DONE")
