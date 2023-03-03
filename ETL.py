from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, lower, upper, initcap, sum, isnull, expr
from pyspark.sql.types import*
import mysql.connector as mariadb

def read_json_file_to_df(spark, filename):
    
    spark = SparkSession.builder.appName('Bank_System').getOrCreate()
    if os.path.exists(filename):
        return spark.read.json(filename)
    else:
        return None
    

def clean_customer_df(spark, filename):
    
    customer_df = read_json_to_df(spark, json_files[0])
    customer_df = customer_df.select(initcap(col('FIRST_NAME')),lower(col('MIDDLE_NAME')),initcap(col('LAST_NAME')),'*')\
                                .drop('MIDDLE_NAME','FIRST_NAME','LAST_NAME')\
                                .withColumnRenamed("lower(MIDDLE_NAME)","MIDDLE_NAME")\
                                .withColumnRenamed("initcap(FIRST_NAME)","FIRST_NAME")\
                                .withColumnRenamed("initcap(LAST_NAME)","LAST_NAME")
    
    # Concatenate Apt. No and street name with a comma as a sperator
    customer_df = customer_df.select(concat(concat('STREET_NAME', lit(", "), 'APT_NO'))\
                                     .alias('FULL_STREET_ADDRESS'), '*')\
                                    .withColumn('CUST_ZIP',col('CUST_ZIP').cast(IntegerType()))\
                                    .drop('APT_NO','STREET_NAME')
    return customer_df
    
def clean_branch_df(spark, filename):
    
    branch_df = read_json_to_df(spark, json_files[1])
    return branch_df


def clean_credit_df(spark, filename):
    
    credit_df = read_json_to_df(spark, json_files[2])
    credit_df = credit_df.withColumn('TIMEDID', expr("make_date(year, month, day)"))
    return credit_df



def customer_df_to_db(spark, database_name):
    
    customer = customer_df.write.format("jdbc") \
    .mode("append") \
    .option("url", f"jdbc:mysql://localhost:3306/carolina") \
    .option("dbtable","CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "abc") \
    .save()
    




def branch_df_to_db(spark, database_name):
        
    branch = clean_branch_df().write.format("jdbc") \
    .mode("append") \
    .option("url", f"jdbc:mysql://localhost:3306/carolina") \
    .option("dbtable","CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "abc") \
    .save()
    

def credit_df_to_db(spark, database_name):
        
    credit = clean_credit_df.write.format("jdbc") \
    .mode("append") \
    .option("url", f"jdbc:mysql://localhost:3306/carolina") \
    .option("dbtable","CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "abc") \
    .save()
    

def create_database(database_name):
    
    # connect to server
    connection = mariadb.connect(
    host='localhost',
    user='root',
    password='abc'
    )
    cursor = connection.cursor(buffered=True)
    # check if
    cursor.execute("CREATE DATABASE IF NOT EXISTS " + database_name)
    return None

def main():
    json_files = [
         "./data/json/CDW_SAPP_CUSTOMER.JSON",
        "./data/json/CDW_SAPP_BRANCH.JSON", 
        "./data/json/CDW_SAPP_CREDITCARD.JSON",
    ]
    
    create_database("dc")
    customer_df_to_db(spark, database_name)
    read_json_file_to_df(spark, filename)
    

if __name__ == "__main__":
    main() 
