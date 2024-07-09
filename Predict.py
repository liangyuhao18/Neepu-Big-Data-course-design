from datetime import timedelta, datetime

import findspark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel

from pyspark.pandas import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, unix_timestamp
import pandas as pd

def generate_future_dates(num_days):
    date = datetime.strptime("2020-01-21", "%Y-%m-%d").date()
    num_days = 843+num_days
    future_dates = [date + timedelta(days=x) for x in range(num_days)]
    return future_dates

if __name__=='__main__':
    findspark.init()
    spark = SparkSession.builder \
        .appName("LinearRegressionWithDate") \
        .getOrCreate()
    date=pd.DataFrame(generate_future_dates(30),columns=['date'])
    date=spark.createDataFrame(date)
    date.show()