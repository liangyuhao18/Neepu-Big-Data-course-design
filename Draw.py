import webbrowser
import findspark
from pyecharts.charts import Line,Bar
from pyecharts import options as opts
from pyspark.sql import SparkSession
from Temtable import createtable
def drawline(title,x_data, y_data,y_Data2):
    line = (Line(init_opts=opts.InitOpts(width="1600px", height="850px")).add_xaxis(x_data)
            .add_yaxis("疫情新增病例",y_data)
            .add_yaxis("疫情死亡病例",y_data2)
            .set_global_opts(
                title_opts=opts.TitleOpts(title=title, subtitle="数据来自kaggle"),
                datazoom_opts=[
                    opts.DataZoomOpts(range_start=0, range_end=100),
                    opts.DataZoomOpts(type_="inside", range_start=0, range_end=100),
                ],
            )
            .render("line_echart.html"))
    webbrowser.open("line_echart.html")
def drawbar(title,x_data,y_data,y_data2):
    bar=(Bar(init_opts=opts.InitOpts(width="1600px", height="850px"))
         .add_xaxis(x_data)
         .add_yaxis("疫情新增病例",y_data)
         .add_yaxis("疫情死亡病例",y_data2)
         .set_global_opts(title_opts=opts.TitleOpts(title=title, subtitle="数据来自kaggle"),
                          datazoom_opts=[
                              opts.DataZoomOpts(range_start=0, range_end=100),
                              opts.DataZoomOpts(type_="inside", range_start=0, range_end=100),
                          ],
                          )
         .render("bar.html")
         )
    webbrowser.open("bar.html")
# 初始化 findspark 和 SparkSession
findspark.init()
spark = SparkSession.builder.appName("BigData").getOrCreate()

# 创建数据表
createtable()
# 从表中选择数据并展示
data = spark.sql("select date,sum(cases) cases,sum(deaths) deaths from table where state='California' group by date order by date asc")
data.show()
# 提取数据到列表
x_data = [row['date'] for row in data.collect()]
y_data = [row['cases'] for row in data.collect()]
y_data2= [row['deaths'] for row in data.collect()]
drawline('美国加州疫情折现图',x_data,y_data,y_data2)

bardata = spark.sql("select state,sum(cases) cases,sum(deaths) deaths from table group by state order by cases asc")
bardata.show()
x_data=[row['state'] for row in bardata.collect()]
y_data=[row['cases'] for row in bardata.collect()]
y_data2=[row['deaths'] for row in bardata.collect()]
drawbar('美国各州疫情柱状图',x_data,y_data,y_data2)