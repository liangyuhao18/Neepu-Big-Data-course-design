import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofweek, month, year, day, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, GBTRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel
from Draw import drawline
from Predict import generate_future_dates
from Temtable import createtable
import pandas as pd


class myGBTRegressionModel:
    def __init__(self,pre):
        findspark.init()
        self.spark = SparkSession.builder.appName("TimeSeriesRegression").getOrCreate()
        createtable()
        self.type=pre

    def load_and_prepare_data(self, data):

        data = data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        data = data.withColumn("year", year(col("date")))
        data = data.withColumn("day_of_week", dayofweek(col("date")))
        data = data.withColumn("month", month(col("date")))
        data = data.withColumn("day", day(col("date")))
        data = data.withColumn("timestamp", unix_timestamp(col("date"), "yyyy-MM-dd"))

        assembler = VectorAssembler(inputCols=["year", "day_of_week", "month", "day", "timestamp"],
                                    outputCol="features")
        data = assembler.transform(data)

        return data

    def train_model(self, data):
        train_data, test_data = data.randomSplit([0.8, 0.2])

        gbt = GBTRegressor(featuresCol="features", labelCol=self.type, maxIter=150)
        self.gbt_model = gbt.fit(train_data)

        print("Feature Importances: " + str(self.gbt_model.featureImportances))

        predictions=self.gbt_model.transform(test_data)
        self.evaluate_model(predictions)

        return train_data, test_data

    def make_predictions(self, day):
        date = generate_future_dates(day)
        date = pd.DataFrame(date, columns=["date"])
        date = self.spark.createDataFrame(date)
        data = self.load_and_prepare_data(date)

        predictions = self.gbt_model.transform(data)

        return predictions

    def plot_predictions(self,data,predictions,title):
        x_data = [row['date'] for row in predictions.collect()]
        y_data = [row[self.type] for row in data.collect()]
        y_data2 = [row['prediction'] for row in predictions.collect()]

        title1=None
        title2=None
        if(self.type=="cases"):
            title1="疫情新增病例"
            title2="疫情预测新增病例"
        elif(self.type=="deaths"):
            title1 = "疫情死亡病例"
            title2 = "疫情预测死亡病例"
        drawline(title+"疫情预测图", x_data, y_data, y_data2,title1,title2)

    def evaluate_model(self, predictions):
        evaluator = RegressionEvaluator(labelCol=self.type, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    def save_model(self, path):
        self.gbt_model.write().overwrite().save(path)
        print(f"Model saved to {path}")

    def load_model(self, path):
        self.gbt_model = GBTRegressionModel.load(path)
        print(f"Model loaded from {path}")

    def stop_spark(self):
        # 停止SparkSession
        self.spark.stop()


if __name__ == "__main__":
    gbt_model = myGBTRegressionModel("cases")
    data = gbt_model.spark.sql("select date, sum(cases) as cases, sum(deaths) as deaths from table group by date order by date")
    data = gbt_model.load_and_prepare_data(data)
    gbt_model.train_model(data)
    predictions = gbt_model.make_predictions(180)
    gbt_model.plot_predictions(data, predictions)
    gbt_model.save_model("./gtr_cases_model")
    gbt_model.stop_spark()
