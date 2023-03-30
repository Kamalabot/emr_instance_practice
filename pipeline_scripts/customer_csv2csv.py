from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import sys

if __name__ == "__main__":

    filePath = "/user/ubuntu/Customers.csv" 

    print(f"The file path provided is {filePath}")

    spark = SparkSession.builder.appName("Customer Transformation").getOrCreate()

    use_schema = StructType([StructField('CustomerID', 
                                             IntegerType(), True), 
                                 StructField('Gender', 
                                             StringType(), True), 
                                 StructField('Age', 
                                             IntegerType(), True), 
                                 StructField('AnnualIncome', 
                                             IntegerType(), True), 
                                 StructField('SpendingScore', 
                                             IntegerType(), True), 
                                 StructField('Profession', 
                                             StringType(), True), 
                                 StructField('WorkExperience', 
                                             IntegerType(), True), 
                                 StructField('FamilySize', 
                                             IntegerType(), True)])

    customer_sdf = spark.read.csv(filePath,
                                  header=True,
                                  schema=use_schema)

    print("Lets check the schema. Just in case...")

    print(customer_sdf.printSchema())

    print("Creating database")

    spark.sql("CREATE DATABASE IF NOT EXISTS customer_spark_db")

    spark.sql("USE customer_spark_db")

    customer_sdf.write.saveAsTable("customer_spark_table",
                                   mode="overwrite",
                                   format='parquet')

    print("Select top 5 rows and printing out in the prompt....")

    spark.sql("""
                SELECT * FROM customer_spark_table
                WHERE AnnualIncome > 15000 AND
                SpendingScore > 50
              """).show(5)

    print("writing the new table to file in home directory")

    spark.sql("""
              SELECT * FROM customer_spark_table
                WHERE AnnualIncome > 15000 AND
                SpendingScore > 50
              """).write.csv("~/transformed.csv",
                             mode="overwrite")

    print("write complete. Check file")
    
    spark.stop()



