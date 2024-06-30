import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

findspark.init()

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("LinearRegressionExample") \
    .getOrCreate()

# 加载数据
file_path = "us-counties.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# 选择特征和标签
data = data.select("cases", "deaths")

# 将数据转换为特征向量
assembler = VectorAssembler(inputCols=["cases"], outputCol="features")
data = assembler.transform(data)

# 划分数据集
train_data, test_data = data.randomSplit([0.8, 0.2])

# 训练线性回归模型
lr = LinearRegression(featuresCol="features", labelCol="deaths")
lr_model = lr.fit(train_data)

# 打印模型参数
print(f"Coefficients: {lr_model.coefficients}")
print(f"Intercept: {lr_model.intercept}")

# 评估模型
test_results = lr_model.evaluate(test_data)
print(f"RMSE: {test_results.rootMeanSquaredError}")
print(f"R2: {test_results.r2}")

# 停止SparkSession
spark.stop()
