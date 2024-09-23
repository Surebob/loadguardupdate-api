# socrata_api.py

import os
import logging
import asyncio
from datetime import datetime
from src.error_handler import APIError
from src.file_manager import FileManager
from config.settings import DATASET_URLS

class SocrataAPI:
    def __init__(self, base_dir):
        self.datasets = DATASET_URLS
        self.base_dir = base_dir
        self.logger = logging.getLogger(__name__)
        self.file_manager = FileManager(base_dir)

    async def check_dataset_update(self, dataset_name):
        if dataset_name not in self.datasets:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        url = self.datasets[dataset_name]
        async with self.file_manager.session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            last_updated = data.get('rowsUpdatedAt')
            if last_updated:
                return datetime.fromtimestamp(last_updated)
            else:
                raise APIError(f"No 'rowsUpdatedAt' field found for dataset {dataset_name}")

    async def update_and_download_datasets(self):
        any_updates = False
        async with self.file_manager:
            for dataset_name in self.datasets:
                try:
                    dataset_dir = os.path.join(self.base_dir, dataset_name)
                    os.makedirs(dataset_dir, exist_ok=True)
                    metadata_file = os.path.join(dataset_dir, f"{dataset_name}_metadata.json")

                    rows_updated_at = await self.check_dataset_update(dataset_name)
                    self.logger.info(f"Server update date for {dataset_name}: {rows_updated_at}")

                    needs_update = True
                    saved_metadata = await self.file_manager.read_metadata(dataset_name)
                    if saved_metadata and 'rowsUpdatedAt' in saved_metadata:
                        local_date = datetime.fromisoformat(saved_metadata['rowsUpdatedAt'])
                        needs_update = rows_updated_at > local_date

                    if needs_update:
                        self.logger.info(f"New update found for {dataset_name}. Downloading dataset.")
                        download_url = f"{self.datasets[dataset_name]}/rows.csv?accessType=DOWNLOAD&api_foundry=true"
                        file_path = os.path.join(dataset_dir, f"{dataset_name}.csv")
                        try:
                            await self.file_manager.download_file(download_url, file_path)
                            await self.file_manager.save_metadata(dataset_name, {
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
                except Exception as e:
                    self.logger.error(f"Error updating {dataset_name}: {str(e)}")

        return any_updates
