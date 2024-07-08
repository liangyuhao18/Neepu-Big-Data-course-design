import requests
import zipfile
import os

def download_and_extract_zip(url, extract_to='./'):
    # Download the file from the URL
    response = requests.get(url)
    zip_filename = 'downloaded_file.zip'

    # Write the content to a file
    with open(zip_filename, 'wb') as f:
        f.write(response.content)

    # Extract the ZIP file
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    # Clean up the downloaded zip file
    os.remove(zip_filename)
if __name__ == '__main__':
    # Example usage
    url = 'https://storage.googleapis.com/kaggle-data-sets/494766/1402868/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240708%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240708T030400Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=0ae7251bec0e2f1b9307dad80423f005fd022ac40d85e168483308069ff18e98937f9e5d8de1e61faa43a91c1031369801e8d2ba26d9128ac97e2853ce54e7d61183b27e8a358a360207e84fb5b3a7da7bb0246aacd5708cabb68d3fed02ca5317118fbcfd821cfb0cc0ba65cc10204e3127840724f8818b7450a886b1f25f080d1c51f43a19985cf7ff438c13caf0882496722bc54bf2dbe4a9d4fd97e4966ab0d8b8267dee8d24aaa8dfa03c9d483abf791c091a50af64baf16b82f421430feca9d065b43de69461c1ed0ee8ae1a6c68f6946e853ae5e765122a8742a9483a650028120525ab7c5fb20e207af8bd8a8df7c5583dc3db8c17f0412d9182e891'
    download_and_extract_zip(url)
