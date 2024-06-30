from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as func
import findspark


findspark.init()
def toDate(inputStr):
    newStr = ""
    if len(inputStr) == 8:
        s1 = inputStr[0:4]
        s2 = inputStr[5:6]
        s3 = inputStr[7]
        newStr = s1 + "-" + "0" + s2 + "-" + "0" + s3
    else:
        s1 = inputStr[0:4]
        s2 = inputStr[5:6]
        s3 = inputStr[7:]
        newStr = s1 + "-" + "0" + s2 + "-" + s3
    date = datetime.strptime(newStr, "%Y-%m-%d")
    return date


#主程序:
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

fields = [StructField("date", DateType(), False),
          StructField("county", StringType(), False),
          StructField("state", StringType(), False),
          StructField("cases", IntegerType(), False),
          StructField("deaths", IntegerType(), False), ]
schema = StructType(fields)

rdd0 = spark.sparkContext.textFile("us-counties.txt")
rdd1 = rdd0.map(lambda x: x.split("\t")).map(lambda p: Row(toDate(p[0]), p[1], p[2], int(p[3]), int(p[4])))

shemaUsInfo = spark.createDataFrame(rdd1, schema)

shemaUsInfo.createOrReplaceTempView("usInfo")

spark.stop()
