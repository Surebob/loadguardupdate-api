import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.error_handler import APIError
from datetime import datetime
import os

class SocrataAPI:
    def __init__(self, base_dir, timeout=10, retries=3):
        self.base_dir = base_dir
        self.session = requests.Session()
        retry = Retry(total=retries, backoff_factor=0.1)
        self.session.mount('https://', HTTPAdapter(max_retries=retry))
        self.timeout = timeout

        self.datasets = {
            'ActPendInsur': 'https://datahub.transportation.gov/api/views/qh9u-swkp',
            'AuthHist': 'https://data.transportation.gov/api/views/9mw4-x3tu',
            'CarrierAllWithHistory': 'https://data.transportation.gov/api/views/6eyk-hxee',
            'NewCompanyCensusFile': 'https://data.transportation.gov/api/views/az4n-8mr2',
            'VehicleInspectionsFile': 'https://data.transportation.gov/api/views/fx4q-ay7w',
            'InspectionPerUnit': 'https://data.transportation.gov/api/views/wt8s-2hbx',
            'InsurAllWithHistory': 'https://data.transportation.gov/api/views/ypjt-5ydn',
            'CrashFile': 'https://datahub.transportation.gov/api/views/aayw-vxb3'
        }

        self.dropbox_datasets = [
            'https://www.dropbox.com/scl/fi/rrn5p8ha4x7wd6bb86gwz/CENSUS_PUB_20240509_1of3.csv?rlkey=wc9j8p0ugmb4o0ngoxs1lku6a&st=cbgzc1sb&dl=1',
            'https://www.dropbox.com/scl/fi/hlbew8zt2v7iha72gn5ce/CENSUS_PUB_20240509_2of3.csv?rlkey=siv1rag8c1875t471l8uussnz&st=wwlbggvj&dl=1',
            'https://www.dropbox.com/scl/fi/zj5tznnlmzqrt21jo71f4/CENSUS_PUB_20240509_3of3.csv?rlkey=ld3z7jgsp26ka9d74ryzkpayp&st=farqx7zi&dl=1'
        ]

    def check_dataset_update(self, dataset_name):
        if dataset_name not in self.datasets:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        try:
            url = self.datasets[dataset_name]
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            last_updated = data.get('rowsUpdatedAt')
            if last_updated:
                return datetime.fromtimestamp(last_updated)
            else:
                raise APIError(f"No 'rowsUpdatedAt' field found for dataset {dataset_name}")
        except requests.RequestException as e:
            raise APIError(f"Failed to check update for dataset {dataset_name}: {str(e)}")

    def download_dataset(self, dataset_name, is_dropbox=False):
        if is_dropbox:
            url = dataset_name
        elif dataset_name in self.datasets:
            url = f"{self.datasets[dataset_name]}/rows.csv?accessType=DOWNLOAD&api_foundry=true"
        else:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise APIError(f"Failed to download dataset {dataset_name}: {str(e)}")

    def update_and_download_datasets(self):
        any_updates = False

        for dataset_name in self.datasets:
            try:
                dataset_dir = os.path.join(self.base_dir, dataset_name)
                os.makedirs(dataset_dir, exist_ok=True)
                file_path = os.path.join(dataset_dir, f"{dataset_name}.csv")

                last_updated = self.check_dataset_update(dataset_name)
                
                if not os.path.exists(file_path) or last_updated > datetime.fromtimestamp(os.path.getmtime(file_path)):
                    print(f"Updating {dataset_name}")
                    data = self.download_dataset(dataset_name)
                    with open(file_path, 'wb') as f:
                        f.write(data)
                    any_updates = True
                else:
                    print(f"{dataset_name} is up to date")
            except APIError as e:
                print(f"Error updating {dataset_name}: {str(e)}")

        if any_updates:
            self.download_dropbox_datasets()

    def download_dropbox_datasets(self):
        for i, url in enumerate(self.dropbox_datasets, 1):
            try:
                file_path = os.path.join(self.base_dir, f"CENSUS_PUB_20240509_{i}of3.csv")
                print(f"Downloading CENSUS_PUB_20240509_{i}of3.csv")
                data = self.download_dataset(url, is_dropbox=True)
                with open(file_path, 'wb') as f:
                    f.write(data)
            except APIError as e:
                print(f"Error downloading CENSUS_PUB_20240509_{i}of3.csv: {str(e)}")