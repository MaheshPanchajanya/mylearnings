# from pyspark.sql import SparkSession

# try:
# 	spark = SparkSession.builder.appName('stationaryanalytics').getOrCreate()
# except:
# 	print("spark session failed to create")

# df_spark = spark.read.csv('product.csv',inferSchema =True,header=True)

# print(df_spark.show())

# print(df_spark.printSchema())

# print(df_spark.dtypes) 

# print(df_spark.describe().show())

# print(df_spark.select(['PNAME','QTY']).show(3))

# print(df_spark.count())

# from pyspark.sql import SparkSession

# sp = SparkSession.builder.appName('myapp').getOrCreate()

# df = sp.read.csv('product.csv',inferSchema=True,header=True)

# print(df)



import pandas as pd

df = pd.read_csv('product.csv',header=1)

df.to_csv('new.csv',index=False)
