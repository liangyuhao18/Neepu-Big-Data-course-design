import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofyear, dayofweek, month, year, day, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from Draw import drawline
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
data = data.withColumn("year", year(col("date")))
data = data.withColumn("day_of_week", dayofweek(col("date")))
data = data.withColumn("month", month(col("date")))
data = data.withColumn("day", day(col("date")))
data = data.withColumn("timestamp", unix_timestamp(col("date"), "yyyy-MM-dd"))

# 特征向量
assembler = VectorAssembler(inputCols=["year", "day_of_week", "month", "day", "timestamp"], outputCol="features")
data = assembler.transform(data)

# 拆分训练和测试数据集
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# 训练随机森林回归模型
rf = RandomForestRegressor(featuresCol="features", labelCol="cases", numTrees=100)
rf_model = rf.fit(train_data)

# 显示模型参数
print("Feature Importances: " + str(rf_model.featureImportances))

# 预测测试数据集
predictions = rf_model.transform(test_data)

# 显示预测结果
predictions.show()

# 提取预测结果用于绘图
x_data = [row['date'] for row in predictions.collect()]
y_data = [row['cases'] for row in predictions.collect()]
y_data2 = [row['prediction'] for row in predictions.collect()]

drawline("预测图", x_data, y_data, y_data2)

# 评估模型
evaluator = RegressionEvaluator(labelCol="cases", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# 停止SparkSession
spark.stop()
