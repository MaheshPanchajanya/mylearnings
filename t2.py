from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('error_handling').getOrCreate()
spark_df = spark.read.csv('emp.csv',header=True,inferSchema=True)

print(spark_df.show())

#----------droping null values from data set----------

#na is the method used to cleanup up or drop null values 

#it has associated methods like fill,replace,drop


# drp_nl = spark_df.na.drop() 

# print(drp_nl.show())

#any and how 

# any : returns true if atleast one value in column satisfies condition

#how: 1 for first non-null record ,2 for last non-null record,3 for all records with a non-null value

# drp_nl = spark_df.na.drop(how='all') 
# print(drp_nl.show())

#threshold  
#dropped rows are those which have more than threshold number of nulls

# drp_nl = spark_df.na.drop(how='any',thresh = 2) 
# print(drp_nl.show())

#subset 
#drops only specified columns

# drp_nl = spark_df.na.drop(columns=['salary']) #only drops salary colummn

# drp_nl = spark_df.na.drop(how ='any',subset=['salary'])
# print(drp_nl.show())

#filling missing values
#fillna replaces null values by some other value

# drp_nl = spark_df.na.replace("missing data",['Experiece','Age'])
# print(drp_nl.show())

#imputer function
#to replace missing values we can use imputer funtion

from pyspark.ml.feature import Imputer

imputer = Imputer(
   inputCols= ['Age','Experiece','Salary'],
   outputCols = ["{}_imputed".format(c) for c in ['Age','Experiece','Salary']]
).setStrategy("mean")

print(imputer.fit(spark_df).transform(spark_df).show())

