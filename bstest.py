import json
import requests
from bs4 import BeautifulSoup

url = 'https://www.kaggle.com/datasets/fireballbyedimyrnmom/us-counties-covid-19-dataset'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
script_tag = soup.find('script', type='application/ld+json')

if script_tag:
    json_content = script_tag.string
    data = json.loads(json_content)
    # 提取contentUrl
    content_urls = [distribution['contentUrl'] for distribution in data['distribution'] if 'contentUrl' in distribution]
    if content_urls:
        file_url = content_urls[0]
        print(f"Downloading from: {file_url}")

        # 下载文件
        response = requests.get(file_url)
        if response.status_code == 200:
            # 保存文件
            with open('dataset.zip', 'wb') as file:
                file.write(response.content)
            print("File downloaded successfully.")
        else:
            print("Failed to download file.")
    else:
        print("No contentUrl found.")
else:
    print("No JSON-LD script tag found.")