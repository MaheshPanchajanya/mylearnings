from pyspark.sql import SparkSession

mlib_sessn =  SparkSession.builder.appName('mlib').getOrCreate()

df_mlib = mlib_sessn.read.csv('testdataset.csv',header=True,inferSchema=True)

from pyspark.ml.feature import VectorAssembler

vectorassembler = VectorAssembler(inputCols=['Age','Experience'],outputCol='independent feature')

output = vectorassembler.transform(df_mlib)

print(output.show())

finalmodel = output.select('independent feature','salary')

print(finalmodel.show())