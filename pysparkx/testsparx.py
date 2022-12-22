from pyspark.sql import SparkSession

session = SparkSession.builder.appName("testapp").getOrCreate()
print(session)