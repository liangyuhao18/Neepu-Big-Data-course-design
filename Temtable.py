import findspark
from pyspark.sql import SparkSession
def createtable():
    findspark.init()
    spark = SparkSession.builder.appName("BigData").getOrCreate()
    data = spark.read.csv("us-counties.csv", header=True,inferSchema=True)
    print(data)
    data.createOrReplaceTempView("table")
