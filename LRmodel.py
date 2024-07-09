import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofweek, month, year, day, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel
from Draw import drawline
from Predict import generate_future_dates
from Temtable import createtable
import pandas as pd


class myLinearRegressionModel:
    def __init__(self,pre):
        findspark.init()
        self.spark = SparkSession.builder.appName("TimeSeriesRegression").getOrCreate()
        createtable()
        self.type=pre

    def load_and_prepare_data(self, data):
        # 加载数据
        data = data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        # 提取特征
        data = data.withColumn("year", year(col("date")))
        data = data.withColumn("day_of_week", dayofweek(col("date")))
        data = data.withColumn("month", month(col("date")))
        data = data.withColumn("day", day(col("date")))
        data = data.withColumn("timestamp", unix_timestamp(col("date"), "yyyy-MM-dd"))

        # 特征向量
        assembler = VectorAssembler(inputCols=["year", "day_of_week", "month", "day", "timestamp"],
                                    outputCol="features")
        data = assembler.transform(data)

        return data

    def train_model(self, data):
        # 拆分训练和测试数据集
        train_data, test_data = data.randomSplit([0.8, 0.2])

        # 训练线性回归模型
        lr = LinearRegression(featuresCol="features", labelCol=self.type)
        self.lr_model = lr.fit(train_data)

        # 显示模型系数和截距
        print("Coefficients: " + str(self.lr_model.coefficients))
        print("Intercept: " + str(self.lr_model.intercept))

        predictions = self.lr_model.transform(test_data)
        self.evaluate_model(predictions)

        return train_data, test_data

    def make_predictions(self, day):
        date = generate_future_dates(day)
        date = pd.DataFrame(date, columns=["date"])
        date = self.spark.createDataFrame(date)
        data = self.load_and_prepare_data(date)
        # 预测测试数据集
        predictions = self.lr_model.transform(data)

        return predictions
    def plot_predictions(self, data, predictions,title):
        # 提取预测结果用于绘图
        x_data = [row['date'] for row in predictions.collect()]
        y_data = [row[self.type] for row in data.collect()]
        y_data2 = [row['prediction'] for row in predictions.collect()]

        title1 = None
        title2 = None
        if (self.type == "cases"):
            title1 = "疫情新增病例"
            title2 = "疫情预测新增病例"
        elif (self.type == "deaths"):
            title1 = "疫情死亡病例"
            title2 = "疫情预测死亡病例"
        drawline(title+"疫情预测图", x_data, y_data, y_data2, title1, title2)

    def evaluate_model(self, predictions):
        # 评估模型
        evaluator = RegressionEvaluator(labelCol=self.type, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    def save_model(self, path):
        # 保存模型
        self.lr_model.write().overwrite().save(path)
        print(f"Model saved to {path}")

    def load_model(self, path):
        # 加载模型
        self.lr_model = LinearRegressionModel.load(path)
        print(f"Model loaded from {path}")

    def stop_spark(self):
        # 停止SparkSession
        self.spark.stop()


# 使用示例
if __name__ == "__main__":
    lr_model = myLinearRegressionModel("deaths")
    data = lr_model.spark.sql("select date, sum(cases) as cases, sum(deaths) as deaths from table group by date order by date")
    data = lr_model.load_and_prepare_data(data)
    train_data, test_data = lr_model.train_model(data)
    predictions = lr_model.make_predictions(1000)
    lr_model.plot_predictions(data,predictions)
    lr_model.save_model("linear_regression_deaths_model")
    lr_model.stop_spark()
