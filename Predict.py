import findspark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel

from pyspark.pandas import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, unix_timestamp


# def predict(path,feature):
#     model=LinearRegressionModel.load(path)
#     return model.transform(feature)
if __name__=='__main__':
    findspark.init()
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("LinearRegressionWithDate") \
        .getOrCreate()
    model = LinearRegressionModel.load("./ts_lr_model_cases")
    data = spark.createDataFrame([
        ("5052-07-01",),  # 示例日期
        ("6565-07-02",)
    ], ["date"])
    data = data.withColumn("date", unix_timestamp(col("date"), "yyyy-MM-dd"))
    assembler_cases = VectorAssembler(inputCols=["date"], outputCol="features")
    assembler_cases = assembler_cases.transform(data)
    assembler_cases.show()
    model.transform(assembler_cases).show()

