import findspark
from pyspark.sql import SparkSession
def createtable():
    findspark.init()
    spark = SparkSession.builder.appName("BigData").getOrCreate()
    data = spark.read.csv("us-counties.csv", header=True,inferSchema=True)
    data.select([col for col in data.columns if data.filter(data[col].isNull()).count() > 0]).show()
    data = data.dropna()
    data.createOrReplaceTempView("table")