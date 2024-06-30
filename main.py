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

# Example usage
url = 'https://storage.googleapis.com/kaggle-data-sets/1687315/4811182/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240627%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240627T100921Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=3549e0f820a2f664018460d9ec12b5e4e164b1dda634b2ac9a4226f6b3c706c948b537f68395edeb4bd60f52235b8c442a61173706344437970f52378513520724518cdbd858ca9d0e593c3313dddf481297683a454aaf3acb88ff2bcee8a827e23d94db08c98e63c8185ec9b080adf7e286bc8c372bc7f39e4c495e000759302fcc53612d03422585b466e1ddc1a11c1af3bc2b1cfb417d0a5e772bc3cf3f019dbcb740e92027a0883db0b18975c49d753b093e53f10339cc924fdcb7adbf04f0e36f4791947bccdefeeb66a0cbfd22321c6fb91c978ec201480a7609864e9b842df00c4765fdcfb653b7c7fe19f539541d246452a792496c52402bf7025ec5'
download_and_extract_zip(url)
