from pyspark.sql import SparkSession

session = SparkSession.builder.appName("testapp").getOrCreate()


df = session.read.csv('pract1.csv',header=True,inferSchema=True)

print(df)

a = df.printSchema()

print(a)

b = df.describe().show()

print(b)

c = df.select(['name','age']).show()

print(c)

d = df.withColumn('exp after 2 years',df['exp']+2).show()

print(d)

e = df.withColumnRenamed('designation','Role').show()

print(e)

x = df.na.drop().show()

print(x)

z = df.na.drop(thresh=3).show()

print(z)

f = df.na.drop(subset='age').show()

print(z)

g = df.na.fill('missing',['age','exp','salary']).show()

print(g)

from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['Age','exp','salary'],
    outputCols=["{}_imputed".format(c) for c in ['Age','exp','salary']]
).setStrategy('mean')

trnsfrm= imputer.fit(df).transform(df).show()
print(trnsfrm)