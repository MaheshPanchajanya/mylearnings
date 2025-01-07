# from pyspark.sql import SparkSession

# ss = SparkSession.builder.appName("sparksession").getOrCreate() 
# # print(ss)
# df = ss.read.csv("emp.csv", inferSchema = True, header = True)
# print(df.show())
# print(df.printSchema())
# print(df.drop("EMPID").show())
# print(df.withColumn("salary with 5% hike",df["SALARY"] * 0.05 + df["SALARY"]).show())
# print(df.na.drop(how = 'any', thresh = None, subset = None).show())
# print(df.filter(df["SALARY"]>65000).show())
# print(df.groupBy("CITY").sum("SALARY").where(df["CITY"]=="Florida").show())



#second handson
#1-number of male and female emps
# 2-emps date of birth between 1995 to 2025
# 3-if phone no bigns with +1 then new column name country should be usa


# from pyspark.sql import SparkSession

# from pyspark.sql.functions import *

# import pandas as pd
# import boto3

# spark = SparkSession.builder\
# .config("spark.jars", "/home/manjiro/Downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar")\
# .appName("sparksession").getOrCreate()
# # print(spark)

# s_client = boto3.client("s3",aws_access_key_id="AKIAVIOZF4MDRQJG5OEW",aws_secret_access_key="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",region_name="ap-south-1")
# response = s_client.get_object(Bucket = "cd-ingest-csv-sindhu",Key = "source-bucket/people-2000000.csv")
# # print(response)
# df = pd.read_csv(response.get("Body"))
# df.info()
# sp_df = spark.createDataFrame(df)


# sp_df.createOrReplaceTempView("table")

# # sql_df = spark.sql("select * from table where Dateofbirth >= 1995-1-1 And Dateofbirth <= 2000-1-1")
# # sql_df = spark.sql("select * from table where (select country = usa as  Phone like '+1%')")
# # print(sql_df.show())

# cf = sp_df.withColumn("country", when(sp_df["Phone"].like("+1%"),"USA").otherwise("na"))

# print(cf.filter(cf["country"]=="USA").show(5))

# import io

# with io.StringIO() as csv_buffer:
#     pf = cf.toPandas()
#     pf.to_csv(csv_buffer,index=False)
#     response = s_client.put_object(Bucket = "cd-ingest-csv-sindhu",Key = "source-bucket/emp_country.csv",Body=csv_buffer.getvalue())
    
    



from pyspark.sql import SparkSession
#import boto3 commenting boto3 is not required sas we are not using S3
#import sys

#sys.path.insert("/home/manjiro/Desktop/my_etl_project/")
#from module_test import *


input_keys()

ss= SparkSession.builder\
.config("spark.jars", "/home/manjiro/Downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar")\
.appName("sparksession").getOrCreate()
print(ss)
df = ss.read.option("header", "true") \
    .option("delimiter", "|") \
    .option("inferSchema", "true") \
    .csv("customer_med.txt")

print(df.show())
df1 = ss.read.option("header", "true") \
    .option("delimiter", "|") \
    .option("inferSchema", "true") \
    .csv("med_plan.txt")
    
print(df1.show())

df.createOrReplaceTempView("customer")
df1.createOrReplaceTempView("medplan")

sql_df = ss.sql("select * from customer c left join medplan m where c.PDTYCODE = m.TYCODE")
# sql_df = ss.sql("select * from customer union select * from medplan order by PDTYCODE")
print(sql_df.show())


sql_df.write.format("jdbc").option("driver","com.mysql.cj.jdbc.Driver")\
.option("url", "jdbc:mysql://localhost:3306/data_warehouse") \
.option("dbtable", "customermed_claims1") \
.option("user", "root") \
.option("password", "1234") \
.mode('overwrite')\
.save()

# df1.write.format("jdbc").option("driver","com.mysql.cj.jdbc.Driver")\
# .option("url", "jdbc:mysql://localhost:3306/data_warehouse") \
# .option("dbtable", "medplan_table") \
# .option("user", "root") \
# .option("password", "1234") \
# .mode('overwrite')\
# .save()

