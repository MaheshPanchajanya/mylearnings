#import spark packages

from pyspark.sql import SparkSession

#----------- create spark session --------------


spark = SparkSession.builder.appName('myapp').getOrCreate()

# # print(spark)

# df_pyspark = spark.read.csv('studentt1.csv')

# # print(df_pyspark)

# print(df_pyspark.show())

# #to make name columns appear as it is

# df_pyspark = spark.read.option('header','true').csv('studentt1.csv',inferSchema= True)

# print(df_pyspark)



# print(df_pyspark.show())

# # return  type of spark data set is always pyspark.sql.dataframe.DataFrame

# print(type(df_pyspark))

# #to print information on columns

# print(df_pyspark.printSchema())

#use of inferschema by default all the types in spark are string to differ use option inferschema = True

#reliable way to ryt spark code in single line implementing all above parameters(header,inferschema)

df_pyspark = spark.read.csv('studentt1.csv',header=True,inferSchema=True)

print(df_pyspark.show())
print(df_pyspark.printSchema())
print(df_pyspark.columns)

#to get top 3 records use head
print(df_pyspark.head(3))

#to select column in spark sql 

print(df_pyspark.select('name'))

print(df_pyspark.select('name').show())

# to select multiple columns pass arguments  inside list 

print(df_pyspark.select(['name','age']).show())

#to check data types

print(df_pyspark.dtypes)

print(df_pyspark.describe().show())

#adding columns in dataframes

dt_prcs = df_pyspark.withColumn('age after two years',df_pyspark['age']+2)
print(dt_prcs.show())

# to drop columns in dataframes
# ans = dt_prcs.drop('age after two years')
# print(ans.show())

#to rename columns

rnm =dt_prcs.withColumnRenamed('age after two years','age after 2 years')

print(rnm.show())

