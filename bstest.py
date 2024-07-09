import json
import requests
from bs4 import BeautifulSoup

url = 'https://www.kaggle.com/datasets/fireballbyedimyrnmom/us-counties-covid-19-dataset'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
script_tag = soup.find('script', type='application/ld+json')

json_content = script_tag.string
data = json.loads(json_content)
# 提取contentUrl
content_urls = [distribution['contentUrl'] for distribution in data['distribution'] if 'contentUrl' in distribution]
print(content_urls)
response02 = requests.get(content_urls[0])
with open('response.txt', 'w') as file:
    # 要写入文件的内容
    content = str(response02.headers)

    # 将内容写入文件
    file.write(content)
