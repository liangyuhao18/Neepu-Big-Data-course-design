import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from Temtable import createtable

findspark.init()

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("LinearRegressionWithDate") \
    .getOrCreate()

# 加载数据
createtable()
data=spark.sql("select date,cases,deaths from table where fips= 53061")
data.show()
# 查看数据结构
data.printSchema()

# 转换日期为时间戳
data = data.withColumn("date", unix_timestamp(col("date"), "yyyy-MM-dd"))
# 处理缺失值
data = data.dropna(subset=["cases", "deaths"])

# 选择特征和标签
data_cases = data.select("date", "cases")
data_deaths = data.select("date", "deaths")

# 将数据转换为特征向量
assembler_cases = VectorAssembler(inputCols=["date"], outputCol="features")
data_cases = assembler_cases.transform(data_cases).select("features", "cases")

assembler_deaths = VectorAssembler(inputCols=["date"], outputCol="features")
data_deaths = assembler_deaths.transform(data_deaths).select("features", "deaths")

# 划分数据集
train_data_cases, test_data_cases = data_cases.randomSplit([0.8, 0.2])
train_data_deaths, test_data_deaths = data_deaths.randomSplit([0.8, 0.2])

# 训练线性回归模型
lr_cases = LinearRegression(featuresCol="features", labelCol="cases", regParam=0.1)
lr_model_cases = lr_cases.fit(train_data_cases)

lr_deaths = LinearRegression(featuresCol="features", labelCol="deaths", regParam=0.1)
lr_model_deaths = lr_deaths.fit(train_data_deaths)

# 打印模型参数
# print(f"Cases Model Coefficients: {lr_model_cases.coefficients}")
# print(f"Cases Model Intercept: {lr_model_cases.intercept}")
# print(f"Deaths Model Coefficients: {lr_model_deaths.coefficients}")
# print(f"Deaths Model Intercept: {lr_model_deaths.intercept}")

# 评估模型
test_results_cases = lr_model_cases.evaluate(test_data_cases)
# print(f"Cases Model RMSE: {test_results_cases.rootMeanSquaredError}")
# print(f"Cases Model R2: {test_results_cases.r2}")

test_results_deaths = lr_model_deaths.evaluate(test_data_deaths)
# print(f"Deaths Model RMSE: {test_results_deaths.rootMeanSquaredError}")
# print(f"Deaths Model R2: {test_results_deaths.r2}")

# 打印系数的显著性检验结果
def print_significance_test(lr_model, label):
    print(f"\n{label} Model Significance Test Results:")
    summary = lr_model.summary
    print(f"Degrees of freedom: {summary.degreesOfFreedom}")
    print(f"Residuals: {summary.residuals.show()}")
    print(f"p-values: {summary.pValues}")
    print(f"t-values: {summary.tValues}")

# 打印cases和deaths模型的显著性检验结果
print_significance_test(lr_model_cases, "Cases")
print_significance_test(lr_model_deaths, "Deaths")

# 停止SparkSession
spark.stop()
