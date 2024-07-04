import findspark
from pyspark.ml.regression import LinearRegression, LinearRegressionModel

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col
from pyspark.ml.feature import VectorAssembler


from Temtable import createtable
if __name__ == "__main__":
    findspark.init()

    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("LinearRegressionWithDate") \
        .getOrCreate()

    # 加载数据
    createtable()
    data=spark.sql("select date,sum(cases) cases,sum(deaths) deaths from table where state ='California' group by date order by date")

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
    print(f"Cases Model Coefficients: {lr_model_cases.coefficients}")
    print(f"Cases Model Intercept: {lr_model_cases.intercept}")
    print(f"Deaths Model Coefficients: {lr_model_deaths.coefficients}")
    print(f"Deaths Model Intercept: {lr_model_deaths.intercept}")

    # 评估模型
    test_results_cases = lr_model_cases.evaluate(test_data_cases)
    print(f"Cases Model RMSE: {test_results_cases.rootMeanSquaredError}")
    print(f"Cases Model R2: {test_results_cases.r2}")

    test_results_deaths = lr_model_deaths.evaluate(test_data_deaths)
    print(f"Deaths Model RMSE: {test_results_deaths.rootMeanSquaredError}")
    print(f"Deaths Model R2: {test_results_deaths.r2}")

    predictions = lr_model_cases.transform(test_data_cases)
    predictions.show()
    # 选择样本进行显示
    lr_model_cases.write().overwrite().save("./ts_lr_model_cases")
    lr_model_deaths.write().overwrite().save("./ts_lr_model.deaths")

    # 停止SparkSession
    spark.stop()
