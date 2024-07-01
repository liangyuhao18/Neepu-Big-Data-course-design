import webbrowser
import findspark
from pyecharts.charts import Line
from pyecharts import options as opts
from pyspark.sql import SparkSession
from Temtable import createtable
def drawline(x_data, y_data,y_Data2):
    line = (Line().add_xaxis(x_data)
            .add_yaxis("疫情新增病例",y_data)
            .add_yaxis("疫情死亡病例",y_data2)
            .set_global_opts(title_opts=opts.TitleOpts(title="美国疫情图", subtitle="数据来自kaggle"),
                datazoom_opts=[
                    opts.DataZoomOpts(range_start=0, range_end=100),
                    opts.DataZoomOpts(type_="inside", range_start=0, range_end=100),
                ],
            )
            .render("line_echart.html"))
    webbrowser.open("line_echart.html")
# 初始化 findspark 和 SparkSession
findspark.init()
spark = SparkSession.builder.appName("BigData").getOrCreate()

# 创建数据表
createtable()
# 从表中选择数据并展示
data = spark.sql("SELECT date, cases,deaths FROM table WHERE fips=17031 ORDER BY date ASC")
data.show()

# 提取数据到列表
x_data = [row['date'] for row in data.collect()]
y_data = [row['cases'] for row in data.collect()]
y_data2= [row['deaths'] for row in data.collect()]
drawline(x_data,y_data,y_data2)