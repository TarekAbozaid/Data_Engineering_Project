import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, lower, upper, initcap, sum, isnull, expr
from pyspark.sql.types import*
import mysql.connector as mariadb

#Build an entry point to Spark SQL application

spark = SparkSession.builder.appName('capstone').getOrCreate()

#Create a function to read json files into spark dataFrame

def jsonfile_into_df(file_name):
    return spark.read.json(file_name)
branch = jsonfile_into_df('./data/json/cdw_sapp_branch.json')
credit = jsonfile_into_df('./data/json/cdw_sapp_credit.json')
custmer = jsonfile_into_df('./data/json/cdw_sapp_custmer.json')

#Mapping Logic, performing necessary converstion before moving spark dataframe into new database

# street and name manipluation
custmer = custmer.select(concat(concat('STREET_NAME', lit(", "), 'APT_NO')).alias('FULL_STREET_ADDRESS'), '*').withColumn('CUST_ZIP',col('CUST_ZIP').cast(IntegerType())).drop('APT_NO','STREET_NAME')
custmer = custmer.select(initcap(col('FIRST_NAME')),lower(col('MIDDLE_NAME')),initcap(col('LAST_NAME')),'*').drop('MIDDLE_NAME','FIRST_NAME','LAST_NAME').withColumnRenamed("lower(MIDDLE_NAME)","MIDDLE_NAME").withColumnRenamed("initcap(FIRST_NAME)","FIRST_NAME").withColumnRenamed("initcap(LAST_NAME)","LAST_NAME")

# Credit_card year,month,day to Date in one column
# credit.withColumn('TIMEID', concat('YEAR','MONTH','DAY'))
credit.withColumn('TIMEDID', expr("make_date(year, month, day)"))

# checking zipcode nullness
branch.filter(branch.BRANCH_ZIP.isNull())
# converting zipcode and branchphone datatype
branch.withColumn('BRANCH_ZIP',col('BRANCH_ZIP').cast(IntegerType()))
branch.withColumn('BRANCH_PHONE', col('BRANCH_PHONE').cast(StringType()))


#converting given phone numbers to list to be able to manipulate.
number = branch.select('BRANCH_PHONE').rdd.flatMap(lambda x: x).collect()

# a function converts phone number to (XXX)XXX-XXXX format
nums = []
def number_to_phone(number): 
    for num in number:
        nums.append('('+'617'+')' + num[3:6] + '-' + num[6:])
    return nums    
u = number_to_phone(number)

# a new spark dataframe with the new formated phone number
b = spark.createDataFrame([(l,) for l in nums], ['BRANCH_PHONE'])
#
branch = branch.drop('BRANCH_PHONE').join(b)

#Make connection to server and create a new database

# connect to server
import mysql.connector as mariadb
connection = mariadb.connect(
    host='localhost',
    user='root',
    password='abc'    
    )
# construct a connection cursor constructor
cursor = connection.cursor(buffered=True)

# create database
cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")

#push spark dataFrames into the new database

# load costmer to fakedb
customer_spark_df = custmer.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable","CDW_SAPP_CUSTOMER") \
  .option("user", "root") \
  .option("password", "abc") \
  .save()

# load branch to fakedb
branch_spark_df = branch.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable","CDW_SAPP_BRANCH") \
  .option("user", "root") \
  .option("password", "abc") \
  .save()

# load dredit_card to fakedb
credit_spark_df = credit.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable","CDW_SAPP_CREDIT_CARD") \
  .option("user", "root") \
  .option("password", "abc") \
  .save()

