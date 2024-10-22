import webbrowser
import findspark
from pyecharts.charts import Line, Bar, Map, Pie
from pyecharts import options as opts
from pyspark.sql import SparkSession
from Temtable import createtable


def drawline(title, x_data, y_data, y_data2,y1axis="疫情新增病例",y2axis="疫情死亡病例"):
    line = (Line(init_opts=opts.InitOpts(width="1600px", height="850px")).add_xaxis(x_data)
            .add_yaxis(y1axis, y_data)
            .add_yaxis(y2axis, y_data2)
            .set_global_opts(
        title_opts=opts.TitleOpts(title=title, subtitle="数据来自kaggle"),
        datazoom_opts=[
            opts.DataZoomOpts(range_start=0, range_end=100),
            opts.DataZoomOpts(type_="inside", range_start=0, range_end=100),
        ],
    )
            .render("line_echart.html"))
    webbrowser.open("line_echart.html")


def drawbar(title, x_data, y_data, y_data2):
    bar = (Bar(init_opts=opts.InitOpts(width="1600px", height="850px"))
           .add_xaxis(x_data)
           .add_yaxis("疫情新增病例", y_data)
           .add_yaxis("疫情死亡病例", y_data2)
           .set_global_opts(title_opts=opts.TitleOpts(title=title, subtitle="数据来自kaggle"),
                            datazoom_opts=[
                                opts.DataZoomOpts(range_start=0, range_end=100),
                                opts.DataZoomOpts(type_="inside", range_start=0, range_end=100),
                            ],
                            )
           .render("bar.html")
           )
    webbrowser.open("bar.html")


def drawmap(title, data,data2):
    NAME_MAP_DATA_US = {
        "Alabama": "Alabama",
        "Alaska": "Alaska",
        "Arizona": "Arizona",
        "Arkansas": "Arkansas",
        "California": "California",
        "Colorado": "Colorado",
        "Connecticut": "Connecticut",
        "Delaware": "Delaware",
        "Florida": "Florida",
        "Georgia": "Georgia",
        "Hawaii": "Hawaii",
        "Idaho": "Idaho",
        "Illinois": "Illinois",
        "Indiana": "Indiana",
        "Iowa": "Iowa",
        "Kansas": "Kansas",
        "Kentucky": "Kentucky",
        "Louisiana": "Louisiana",
        "Maine": "Maine",
        "Maryland": "Maryland",
        "Massachusetts": "Massachusetts",
        "Michigan": "Michigan",
        "Minnesota": "Minnesota",
        "Mississippi": "Mississippi",
        "Missouri": "Missouri",
        "Montana": "Montana",
        "Nebraska": "Nebraska",
        "Nevada": "Nevada",
        "New Hampshire": "New Hampshire",
        "New Jersey": "New Jersey",
        "New Mexico": "New Mexico",
        "New York": "New York",
        "North Carolina": "North Carolina",
        "North Dakota": "North Dakota",
        "Ohio": "Ohio",
        "Oklahoma": "Oklahoma",
        "Oregon": "Oregon",
        "Pennsylvania": "Pennsylvania",
        "Rhode Island": "Rhode Island",
        "South Carolina": "South Carolina",
        "South Dakota": "South Dakota",
        "Tennessee": "Tennessee",
        "Texas": "Texas",
        "Utah": "Utah",
        "Vermont": "Vermont",
        "Virginia": "Virginia",
        "Washington": "Washington",
        "West Virginia": "West Virginia",
        "Wisconsin": "Wisconsin",
        "Wyoming": "Wyoming"
    }
    map = (Map(init_opts=opts.InitOpts(width="1600px", height="850px"))
           .add(
        series_name="美国各州累计新增病例图",
        maptype="美国",
        data_pair=data,
        name_map=NAME_MAP_DATA_US,
        is_map_symbol_show=False,
        )
        .add(
        series_name="美国各州累计死亡图",
        maptype="美国",
        data_pair=data2,
        name_map=NAME_MAP_DATA_US,
        is_map_symbol_show=False,
        )
           .set_global_opts(
        title_opts=opts.TitleOpts(
            title="美国各州累计新增病例图",
            subtitle="数据集来自kaggle",
        ),
        tooltip_opts=opts.TooltipOpts(
            trigger="item", formatter="{b}<br/>{c} cases"
        ),
        visualmap_opts=opts.VisualMapOpts(
            min_=0,
            max_=3000000000,
            range_text=["High", "Low"],
            is_calculable=True,
            range_color=["lightskyblue", "yellow", "orangered"],
        ),
    )
           .render("map.html")
           )
    webbrowser.open("map.html")
def drawpie(title,data):
    pie = (
        Pie(init_opts=opts.InitOpts(width="1600px", height="850px"))
        .add(
            "",
            data,
            center=["40%", "50%"],
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical"),
        )
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
        .render("pie_scroll_legend.html")
    )
    webbrowser.open("pie_scroll_legend.html")
if __name__ == '__main__':
    findspark.init()
    spark = SparkSession.builder.appName("BigData").getOrCreate()

    createtable()
    data = spark.sql(
        "select date,sum(cases) cases,sum(deaths) deaths from table where state='California' group by date order by date asc")
    data.show()
    x_data = [row['date'] for row in data.collect()]
    y_data = [row['cases'] for row in data.collect()]
    y_data2 = [row['deaths'] for row in data.collect()]
    drawline('美国加州疫情折线图', x_data, y_data, y_data2)
    #
    bardata = spark.sql("select state,sum(cases) cases,sum(deaths) deaths from table group by state order by cases asc")
    bardata.show()
    x_data = [row['state'] for row in bardata.collect()]
    y_data = [row['cases'] for row in bardata.collect()]
    y_data2 = [row['deaths'] for row in bardata.collect()]
    drawbar('从2020-01-21到2022-05-13美国各州疫情柱状图', x_data, y_data, y_data2)

    mapdata = spark.sql("select state,sum(cases) cases from table group by state order by cases")
    mapdata2 = spark.sql("select state,sum(deaths) deaths from table group by state order by deaths")
    mpdata = mapdata.collect()
    mpdata2 = mapdata2.collect()
    drawmap("图", mpdata,mpdata2)
    drawpie("图",mpdata)
