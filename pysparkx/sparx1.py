from pyspark.sql import SparkSession

session = SparkSession.builder.appName("testapp").getOrCreate()


df = session.read.option('header','true').csv('pract1.csv').show()

print(df)

df = session.read.option('header','true').csv('pract1.csv',inferSchema=True).printSchema()

print(df)

df = session.read.option('header','true').csv('pract1.csv',inferSchema=True).describe().show()

print(df)   

df = session.read.option('header','true').csv('pract1.csv',inferSchema=True).select(['name','age']).show()

print(df)