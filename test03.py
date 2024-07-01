from pyspark import SparkConf,SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as func
from pyspark.sql.functions import sum as _sum, date_add, round
import findspark

findspark.init()
# 初始化SparkSession
spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

# 读取CSV文件
df = spark.read.csv("us-counties.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("usInfo")


df.createOrReplaceTempView("usInfo")

# 1. 计算每日的累计确诊病例数和死亡数
df1 = spark.sql("SELECT date, SUM(cases) AS cases, SUM(deaths) AS deaths FROM usInfo GROUP BY date ORDER BY date")
df1.coalesce(1).write.csv("result/result1.csv", header=True, mode="overwrite")


# 注册为临时表供下一步使用
df1.createOrReplaceTempView("ustotal")

# 2. 计算每日较昨日的新增确诊病例数和死亡病例数
df2 = spark.sql("SELECT t1.date, t1.cases - t2.cases AS caseIncrease, "
                "t1.deaths - t2.deaths AS deathIncrease FROM ustotal t1, ustotal t2 WHERE t1.date = date_add(t2.date, 1) order by t1.date")
df2.coalesce(1).write.csv("result/result2.csv", header=True, mode="overwrite")


# 3. 统计截止2020年5.19日美国各州的累计确诊人数,死亡人数和死亡率
df3 = spark.sql("SELECT date, state, sum(cases) AS totalCases, "
                "sum(deaths) AS totalDeaths, round(sum(deaths) / sum(cases), 4) AS deathRate "
                "FROM usInfo WHERE date = '2020-05-19' GROUP BY date, state")
df3.coalesce(1).write.csv("result/result3.csv", header=True, mode="overwrite")

df3.createOrReplaceTempView("eachStateInfo")


# 4. 找出美国确诊最多的10个州
df4 = spark.sql("SELECT date, state, totalCases FROM eachStateInfo ORDER BY totalCases DESC LIMIT 10")
df4.coalesce(1).write.csv("result/result4.csv", header=True, mode="overwrite")


# 5. 找出美国死亡最多的10个州
df5 = spark.sql("SELECT date, state, totalDeaths FROM eachStateInfo ORDER BY totalDeaths DESC LIMIT 10")
df5.coalesce(1).write.csv("result/result5.csv", header=True, mode="overwrite")


# 6. 找出美国确诊最少的10个州
df6 = spark.sql("SELECT date, state, totalCases FROM eachStateInfo ORDER BY totalCases ASC LIMIT 10")
df6.coalesce(1).write.csv("result/result6.csv", header=True, mode="overwrite")


# 7. 找出美国死亡最少的10个州
df7 = spark.sql("SELECT date, state, totalDeaths FROM eachStateInfo ORDER BY totalDeaths ASC LIMIT 10")
df7.coalesce(1).write.csv("result/result7.csv", header=True, mode="overwrite")


# 8. 统计截止2020年5.19全美和各州的病死率
df8 = spark.sql("SELECT 1 AS sign, date, 'USA' AS state, round(sum(totalDeaths) / sum(totalCases), 4) AS deathRate "
                "FROM eachStateInfo GROUP BY date UNION SELECT 2 AS sign, date, state, deathRate FROM eachStateInfo")
df8.coalesce(1).write.csv("result/result8.csv", header=True, mode="overwrite")


spark.stop()


