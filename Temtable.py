import findspark
from pyspark.sql import SparkSession
findspark.init()
spark=SparkSession.builder.appName("BigData").getOrCreate()
data=spark.read.csv("us-counties.csv",header=True)
data.createOrReplaceTempView("table")
result=spark.sql("select * from table where state='Washington'")
result.show()