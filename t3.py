from pyspark.sql import SparkSession    

sprk_sessn = SparkSession.builder.appName('filter_apps').getOrCreate()

df = sprk_sessn.read.csv('testdataset.csv',header=True,inferSchema = True)

print(df.show())

#spark filter function

fltr_rslt = df.filter("salary<=30000").select(['Name','experience','salary'])

print(fltr_rslt.show())

fltr_rslt = df.filter(~(df['Salary']<=20000) & (df['experience']<=3))
print(fltr_rslt.show())




