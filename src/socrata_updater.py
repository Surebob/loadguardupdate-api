# src/socrata_updater.py
import json
import os
import logging
import aiohttp
import aiofiles
from datetime import datetime
from src.error_handler import APIError, FileError
from config.settings import DATA_DIR, DATASET_URLS
from src.utils import ProgressBar

class SocrataUpdater:
    def __init__(self, session):
        self.datasets = DATASET_URLS
        self.base_dir = DATA_DIR
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = session

    async def update_and_download_datasets(self):
        any_updates = False
        for dataset_name, dataset_url in self.datasets.items():
            try:
                dataset_dir = os.path.join(self.base_dir, dataset_name)
                os.makedirs(dataset_dir, exist_ok=True)
                metadata_file = os.path.join(dataset_dir, f"{dataset_name}_metadata.json")

                rows_updated_at = await self.check_dataset_update(dataset_url)
                self.logger.info(f"Server update date for {dataset_name}: {rows_updated_at}")

                needs_update = True
                saved_metadata = await self.read_metadata(metadata_file)
                if saved_metadata and 'rowsUpdatedAt' in saved_metadata:
                    local_date = datetime.fromisoformat(saved_metadata['rowsUpdatedAt'])
                    needs_update = rows_updated_at > local_date

                if needs_update:
                    self.logger.info(f"New update found for {dataset_name}. Downloading dataset.")
                    download_url = f"{dataset_url}/rows.csv?accessType=DOWNLOAD&api_foundry=true"
                    file_path = os.path.join(dataset_dir, f"{dataset_name}.csv")
                    try:
                        await self.download_file(download_url, file_path)
                        await self.save_metadata(metadata_file, {
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

    async def check_dataset_update(self, url):
        async with self.session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            last_updated = data.get('rowsUpdatedAt')
            if last_updated:
                return datetime.fromtimestamp(last_updated)
            else:
                raise APIError(f"No 'rowsUpdatedAt' field found for dataset at {url}")

    async def download_file(self, url, local_path):
        self.logger.info(f"Downloading {url} to {local_path}")
        progress = ProgressBar(f"Downloading {os.path.basename(local_path)}")
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                total_size = 0
                progress.start()
                async with aiofiles.open(local_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(1024*1024):  # 1MB chunks
                        await f.write(chunk)
                        total_size += len(chunk)
                        progress.update(total_size)
            progress.finish()
        except Exception as e:
            progress.finish()
            raise APIError(f"Failed to download {url}: {str(e)}")

    async def read_metadata(self, metadata_file):
        try:
            if os.path.exists(metadata_file):
                async with aiofiles.open(metadata_file, 'r') as f:
                    content = await f.read()
                    return json.loads(content)
            return None
        except Exception as e:
            raise FileError(f"Failed to read metadata from {metadata_file}: {str(e)}")

    async def save_metadata(self, metadata_file, metadata):
        try:
            async with aiofiles.open(metadata_file, 'w') as f:
                await f.write(json.dumps(metadata, indent=2))
        except Exception as e:
            raise FileError(f"Failed to save metadata to {metadata_file}: {str(e)}")
