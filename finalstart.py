import os.path
import sys

import findspark
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QThread
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from pyspark.sql import SparkSession

from Draw import drawline, drawbar, drawmap, drawpie
from GTRmodel import myGBTRegressionModel
from LRmodel import  myLinearRegressionModel
from RFRmodel import myRandomForestModel
from download import download_and_extract_zip
from final01 import Ui_Form
from Temtable import createtable

states = {
    "美国全境": "America",
    "阿拉巴马州": "Alabama",
    "阿拉斯加州": "Alaska",
    "亚利桑那州": "Arizona",
    "阿肯色州": "Arkansas",
    "加利福尼亚州": "California",
    "科罗拉多州": "Colorado",
    "康涅狄格州": "Connecticut",
    "特拉华州": "Delaware",
    "佛罗里达州": "Florida",
    "乔治亚州": "Georgia",
    "夏威夷州": "Hawaii",
    "爱达荷州": "Idaho",
    "伊利诺伊州": "Illinois",
    "印第安纳州": "Indiana",
    "爱荷华州": "Iowa",
    "堪萨斯州": "Kansas",
    "肯塔基州": "Kentucky",
    "路易斯安那州": "Louisiana",
    "缅因州": "Maine",
    "马里兰州": "Maryland",
    "马萨诸塞州": "Massachusetts",
    "密歇根州": "Michigan",
    "明尼苏达州": "Minnesota",
    "密西西比州": "Mississippi",
    "密苏里州": "Missouri",
    "蒙大拿州": "Montana",
    "内布拉斯加州": "Nebraska",
    "内华达州": "Nevada",
    "新罕布什尔州": "New Hampshire",
    "新泽西州": "New Jersey",
    "新墨西哥州": "New Mexico",
    "纽约州": "New York",
    "北卡罗来纳州": "North Carolina",
    "北达科他州": "North Dakota",
    "俄亥俄州": "Ohio",
    "俄克拉何马州": "Oklahoma",
    "俄勒冈州": "Oregon",
    "宾夕法尼亚州": "Pennsylvania",
    "罗德岛州": "Rhode Island",
    "南卡罗来纳州": "South Carolina",
    "南达科他州": "South Dakota",
    "田纳西州": "Tennessee",
    "德克萨斯州": "Texas",
    "犹他州": "Utah",
    "佛蒙特州": "Vermont",
    "弗吉尼亚州": "Virginia",
    "华盛顿州": "Washington",
    "西弗吉尼亚州": "West Virginia",
    "威斯康星州": "Wisconsin",
    "怀俄明州": "Wyoming"
}
class HistoryThread(QThread):
    def __init__(self,date1,date2,state,main):
        super().__init__()
        self.starttime=date1
        self.endtime=date2
        self.state=state
        self.mainpage=main
    def run(self):
        if self.state=="美国全境":
            data=self.mainpage.spark.sql("select date,sum(cases) cases,sum(deaths) deaths from table"
                                         " where date between {start} and {end}"
                                         " group by date"
                                         " order by date asc",start=self.starttime,end=self.endtime)
            x_data = [row['date'] for row in data.collect()]
            y_data = [row['cases'] for row in data.collect()]
            y_data2 = [row['deaths'] for row in data.collect()]
            drawline(self.state + "疫情图", x_data, y_data, y_data2)
            bardata=self.mainpage.spark.sql("select state,sum(cases) cases,sum(deaths) deaths from table"
                                            " where date between {start} and {end}"
                                            " group by state order by cases asc",start=self.starttime,end=self.endtime)
            x_data = [row['state'] for row in bardata.collect()]
            y_data = [row['cases'] for row in bardata.collect()]
            y_data2 = [row['deaths'] for row in bardata.collect()]
            drawbar('从'+self.starttime+'至'+self.endtime+'美国各州疫情柱状图', x_data, y_data, y_data2)
            mapdata = self.mainpage.spark.sql("select state,sum(cases) cases from table where date between {start} and {end} group by state order by cases",start=self.starttime,end=self.endtime)
            mapdata2 = self.mainpage.spark.sql("select state,sum(deaths) deaths from table where date between {start} and {end} group by state order by deaths",start=self.starttime,end=self.endtime)
            mpdata = mapdata.collect()
            mpdata2 = mapdata2.collect()
            drawmap("美国疫情图", mpdata, mpdata2)
            drawpie("美国各州疫情情况饼图", mpdata)
        else:
            data=self.mainpage.spark.sql("select date,sum(cases) cases,sum(deaths) deaths"
                                " from table"
                                " where state={zhou}"
                                " and date between {start} and {end}"
                                " group by date"
                                " order by date asc",zhou=states[self.state],start=self.starttime,end=self.endtime)
            x_data = [row['date'] for row in data.collect()]
            y_data = [row['cases'] for row in data.collect()]
            y_data2 = [row['deaths'] for row in data.collect()]
            drawline(self.state+"疫情图",x_data,y_data,y_data2)
            bardata=self.mainpage.spark.sql("select county,sum(cases) cases,sum(deaths) deaths from table"
                                            " where date between {start} and {end}"
                                            " and state = {zhou}"
                                            " group by county order by cases asc",zhou=states[self.state],start=self.starttime,end=self.endtime)
            x_data = [row['county'] for row in bardata.collect()]
            y_data = [row['cases'] for row in bardata.collect()]
            y_data2 = [row['deaths'] for row in bardata.collect()]
            drawbar('从'+self.starttime+'至'+self.endtime+"美国"+self.state+"疫情柱状图",x_data,y_data,y_data2)
            piedata=self.mainpage.spark.sql("select county,sum(cases) cases from table where state = {zhou} and date between {start} and {end} group by county order by cases",zhou=states[self.state],start=self.starttime,end=self.endtime)
            piedata = piedata.collect()
            drawpie("美国"+self.state+"疫情情况饼图",piedata)

class PredictThread(QThread):
    def __init__(self,day,model,state,type,mainpage):
        super().__init__()
        self.day=day
        if(type=="病例数"):
            self.type="cases"
        else:
            self.type="deaths"
        if(model=="线性回归模型"):
            self.model=myLinearRegressionModel(self.type)
            self.modelpath="./model/lr_model_"+states[state]+"_"+self.type
        elif(model=="随机森林回归模型"):
            self.model=myRandomForestModel(self.type)
            self.modelpath="./model/rf_model_"+states[state]+"_"+self.type
        elif(model=="梯度上升决策树回归模型"):
            self.model=myGBTRegressionModel(self.type)
            self.modelpath="./model/gbt_model_"+states[state]+"_"+self.type
        self.state=state
        self.mainpage=mainpage
    def run(self):
        data=None
        if(self.state=="美国全境"):
            data=self.mainpage.spark.sql("select date,sum(cases) cases,sum(deaths) deaths from table group by date order by date asc")
        else:
            data=self.mainpage.spark.sql("select date,sum(cases) cases,sum(deaths) deaths from table where state={zhou} group by date order by date asc",zhou=states[self.state])
        data = self.model.load_and_prepare_data(data)
        if os.path.exists(self.modelpath):
            self.model.load_model(self.modelpath)
            predict=self.model.make_predictions(self.day)
            self.model.plot_predictions(data,predict,self.state)
        else:
            self.model.train_model(data)
            predeict=self.model.make_predictions(self.day)
            self.model.plot_predictions(data,predeict,self.state)
            self.model.save_model(self.modelpath)




class MyMainWindows(QMainWindow, Ui_Form):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        findspark.init()
        self.spark = SparkSession.builder.appName("BigData").getOrCreate()
        createtable()

    def showhistory(self):
        date1 = self.dateEdit.date().toString("yyyy-MM-dd")
        date2 = self.dateEdit_2.date().toString("yyyy-MM-dd")
        state = self.comboBox.currentText()
        self.thread=HistoryThread(date1,date2,state,self)
        self.thread.start()

    def getdata(self):
        url = 'https://storage.googleapis.com/kaggle-data-sets/494766/1402868/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240708%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240708T030400Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0ae7251bec0e2f1b9307dad80423f005fd022ac40d85e168483308069ff18e98937f9e5d8de1e61faa43a91c1031369801e8d2ba26d9128ac97e2853ce54e7d61183b27e8a358a360207e84fb5b3a7da7bb0246aacd5708cabb68d3fed02ca5317118fbcfd821cfb0cc0ba65cc10204e3127840724f8818b7450a886b1f25f080d1c51f43a19985cf7ff438c13caf0882496722bc54bf2dbe4a9d4fd97e4966ab0d8b8267dee8d24aaa8dfa03c9d483abf791c091a50af64baf16b82f421430feca9d065b43de69461c1ed0ee8ae1a6c68f6946e853ae5e765122a8742a9483a650028120525ab7c5fb20e207af8bd8a8df7c5583dc3db8c17f0412d9182e891'
        download_and_extract_zip(url,"./dataset")

    def predict(self):
        day=self.spinBox.value()
        model=self.comboBox_3.currentText()
        state=self.comboBox_2.currentText()
        type=self.comboBox_4.currentText()
        self.thread=PredictThread(day,model,state,type,self)
        self.thread.start()


if __name__ == '__main__':
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
    app = QtWidgets.QApplication(sys.argv)
    mainWindow = MyMainWindows()
    mainWindow.show()
    sys.exit(app.exec())
