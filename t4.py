from pyspark.sql import SparkSession

agg_sessn =  SparkSession.builder.appName('Agg').getOrCreate()

df_spark = agg_sessn.read.csv('testdataset2.csv',header=True,inferSchema=True)

print(df_spark.show())


#aggregate fumction groupBy clause

df_spark1 = df_spark.groupBy('Salary').sum().show()
print(df_spark1)

df_spark2 = df_spark.groupBy('Name').sum().show()
print(df_spark2)

df_spark3 = df_spark.groupBy('departments').mean().show()
print(df_spark3)

df_spark4 = df_spark.filter(df_spark['departments']=='IOT').groupBy('departments').count().show()
print(df_spark4)




