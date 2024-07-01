from pyecharts import options as opts
from pyecharts.charts import Map
from pyecharts.faker import Faker
c = (
    Map()
    .add([1,2],"china")
    .set_global_opts(title_opts=opts.TitleOpts(title="Map-基本示例"))
    .render("map_base.html")
)