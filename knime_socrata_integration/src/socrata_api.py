import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.error_handler import APIError

class SocrataAPI:
    def __init__(self, base_url, timeout=10, retries=3):
        self.base_url = base_url
        self.session = requests.Session()
        retry = Retry(total=retries, backoff_factor=0.1)
        self.session.mount('https://', HTTPAdapter(max_retries=retry))
        self.timeout = timeout

    def get_dataset_metadata(self, dataset_id):
        try:
            url = f"{self.base_url}/api/views/{dataset_id}.json"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise APIError(f"Failed to get metadata for dataset {dataset_id}: {str(e)}")

    def download_dataset(self, dataset_id, limit=None):
        try:
            url = f"{self.base_url}/resource/{dataset_id}.csv"
            params = {"$limit": limit} if limit else {}
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise APIError(f"Failed to download dataset {dataset_id}: {str(e)}")