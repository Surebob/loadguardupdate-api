# src/dropbox_handler.py

import os
import requests
from src.error_handler import APIError

class DropboxHandler:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.dropbox_datasets = [
            'https://www.dropbox.com/scl/fi/rrn5p8ha4x7wd6bb86gwz/CENSUS_PUB_20240509_1of3.csv?rlkey=wc9j8p0ugmb4o0ngoxs1lku6a&st=cbgzc1sb&dl=1',
            'https://www.dropbox.com/scl/fi/hlbew8zt2v7iha72gn5ce/CENSUS_PUB_20240509_2of3.csv?rlkey=siv1rag8c1875t471l8uussnz&st=wwlbggvj&dl=1',
            'https://www.dropbox.com/scl/fi/zj5tznnlmzqrt21jo71f4/CENSUS_PUB_20240509_3of3.csv?rlkey=ld3z7jgsp26ka9d74ryzkpayp&st=farqx7zi&dl=1'
        ]

    def download_datasets(self):
        for i, url in enumerate(self.dropbox_datasets, 1):
            try:
                file_path = os.path.join(self.base_dir, f"CENSUS_PUB_20240509_{i}of3.csv")
                print(f"Downloading CENSUS_PUB_20240509_{i}of3.csv")
                response = requests.get(url)
                response.raise_for_status()
                with open(file_path, 'wb') as f:
                    f.write(response.content)
            except requests.RequestException as e:
                raise APIError(f"Error downloading CENSUS_PUB_20240509_{i}of3.csv: {str(e)}")