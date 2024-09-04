import os
import sys
import aiohttp
from datetime import datetime
import logging
import asyncio
from src.error_handler import APIError
from src.file_manager import FileManager
from config.settings import DATASET_URLS

# Configure logging to print to console and write to a file
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("socrata_api.log"),
                        logging.StreamHandler(sys.stdout)
                    ])

class SocrataAPI:
    def __init__(self, base_dir):
        self.datasets = DATASET_URLS
        self.base_dir = base_dir
        self.logger = logging.getLogger(__name__)

    async def check_dataset_update(self, dataset_name):
        if dataset_name not in self.datasets:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        try:
            url = self.datasets[dataset_name]
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    last_updated = data.get('rowsUpdatedAt')
                    if last_updated:
                        return datetime.fromtimestamp(last_updated)
                    else:
                        raise APIError(f"No 'rowsUpdatedAt' field found for dataset {dataset_name}")
        except aiohttp.ClientError as e:
            raise APIError(f"Failed to check update for dataset {dataset_name}: {str(e)}")

    async def update_and_download_datasets(self):
        any_updates = False
        async with FileManager(self.base_dir) as fm:
            for dataset_name, url in self.datasets.items():
                try:
                    dataset_dir = os.path.join(self.base_dir, dataset_name)
                    os.makedirs(dataset_dir, exist_ok=True)
                    metadata_file = os.path.join(dataset_dir, f"{dataset_name}_metadata.json")

                    rows_updated_at = await self.check_dataset_update(dataset_name)
                    self.logger.info(f"Server update date for {dataset_name}: {rows_updated_at}")

                    needs_update = True
                    if os.path.exists(metadata_file):
                        saved_metadata = await fm.read_metadata(dataset_name)
                        if saved_metadata and 'rowsUpdatedAt' in saved_metadata:
                            local_date = datetime.fromisoformat(saved_metadata['rowsUpdatedAt'])
                            needs_update = rows_updated_at > local_date

                    if needs_update:
                        self.logger.info(f"New update found for {dataset_name}. Downloading dataset.")
                        download_url = f"{url}/rows.csv?accessType=DOWNLOAD&api_foundry=true"
                        file_path = os.path.join(dataset_dir, f"{dataset_name}.csv")
                        try:
                            await fm._download_with_progress(download_url, file_path)
                            await fm.save_metadata(dataset_name, {
                                'rowsUpdatedAt': rows_updated_at.isoformat()
                            })
                            self.logger.info(f"Dataset {dataset_name} updated successfully.")
                            any_updates = True
                        except APIError as download_error:
                            self.logger.error(f"Failed to download {dataset_name}: {str(download_error)}")
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            continue
                    else:
                        self.logger.info(f"No updates for dataset {dataset_name}.")
                except APIError as e:
                    self.logger.error(f"Error updating {dataset_name}: {str(e)}")

        return any_updates