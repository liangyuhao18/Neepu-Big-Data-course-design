import findspark
from pyspark.sql import SparkSession
from math import pi
from pyspark.sql.functions import col, to_date, dayofyear, dayofweek, month, sin, cos
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

from Temtable import createtable

findspark.init()
# 初始化SparkSession
spark = SparkSession.builder.appName("TimeSeriesRegression").getOrCreate()
createtable()
# 加载数据
data = spark.sql("select date, sum(cases) as cases, sum(deaths) as deaths from table group by date order by date")
# 转换date列为日期格式
data = data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# 显示数据
data.show()
# 提取特征
data = data.withColumn("day_of_year", dayofyear(col("date")))
data = data.withColumn("day_of_week", dayofweek(col("date")))
data = data.withColumn("month", month(col("date")))

# 使用sin和cos进行周期性转换
data = data.withColumn("day_of_year_sin", sin(2 * pi * col("day_of_year") / 365.25))
data = data.withColumn("day_of_year_cos", cos(2 * pi * col("day_of_year") / 365.25))
data = data.withColumn("day_of_week_sin", sin(2 * pi * col("day_of_week") / 7))
data = data.withColumn("day_of_week_cos", cos(2 * pi * col("day_of_week") / 7))
data = data.withColumn("month_sin", sin(2 * pi * col("month") / 12))
data = data.withColumn("month_cos", cos(2 * pi * col("month") / 12))

# 特征向量
assembler = VectorAssembler(inputCols=["day_of_year_sin", "day_of_year_cos", "day_of_week_sin", "day_of_week_cos", "month_sin", "month_cos"], outputCol="features")
data = assembler.transform(data)

# 显示提取的特征
data.select("date", "features").show(truncate=False)
# 拆分训练和测试数据集
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# 训练线性回归模型
lr = LinearRegression(featuresCol="features", labelCol="cases")
lr_model = lr.fit(train_data)

# 显示模型系数和截距
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))
# 预测测试数据集
predictions = lr_model.transform(test_data)

# 选择样本进行显示
predictions.select("date","cases", "prediction").show()

# 评估模型
evaluator = RegressionEvaluator(labelCol="cases", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

