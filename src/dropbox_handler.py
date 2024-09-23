# dropbox_handler.py

import os
import logging
import re
from datetime import datetime
from src.error_handler import APIError
from src.file_manager import FileManager
from config.settings import DROPBOX_DATASETS

class DropboxHandler:
    def __init__(self, base_dir):
        self.base_dir = os.path.join(base_dir, "CENSUS_FILES")
        self.logger = logging.getLogger(__name__)
        self.dropbox_datasets = DROPBOX_DATASETS
        self.file_manager = FileManager(self.base_dir)

    async def download_datasets(self):
        results = []
        os.makedirs(self.base_dir, exist_ok=True)
        async with self.file_manager:
            for url in self.dropbox_datasets:
                try:
                    filename = os.path.basename(url.split('?')[0])
                    file_path = os.path.join(self.base_dir, filename)

                    remote_date = self._extract_date_from_filename(filename)
                    local_files = [f for f in os.listdir(self.base_dir) if f.endswith('.csv')]
                    latest_local_file = max(local_files, key=lambda f: self._extract_date_from_filename(f), default=None)

                    if latest_local_file:
                        local_date = self._extract_date_from_filename(latest_local_file)
                        if remote_date and local_date and remote_date <= local_date:
                            self.logger.info(f"No update needed for {filename}")
                            results.append((url, False))
                            continue

                    self.logger.info(f"Downloading {filename}")
                    await self.file_manager.download_file(url, file_path)
                    results.append((url, True))
                except APIError as e:
                    self.logger.error(f"Error downloading {filename}: {str(e)}")
                    results.append((url, False))
        return results

    def _extract_date_from_filename(self, filename):
        try:
            match = re.search(r'CENSUS_PUB_(\d{8})', filename)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%Y%m%d')
            else:
                self.logger.warning(f"Unexpected filename format: {filename}")
                return None
        except Exception as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None
