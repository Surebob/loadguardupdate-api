# src/dropbox_handler.py

import os
from datetime import datetime
from src.error_handler import APIError
import logging
from src.file_manager import FileManager
from config.settings import DROPBOX_DATASETS

class DropboxHandler:
    def __init__(self, base_dir):
        self.base_dir = os.path.join(base_dir, "CENSUS_FILES")
        self.logger = logging.getLogger(__name__)
        self.dropbox_datasets = DROPBOX_DATASETS

    async def download_datasets(self):
        results = []
        os.makedirs(self.base_dir, exist_ok=True)
        async with FileManager(self.base_dir) as fm:
            for url in self.dropbox_datasets:
                try:
                    filename = os.path.basename(url.split('?')[0])
                    file_path = os.path.join(self.base_dir, filename)
                    
                    remote_date = self._extract_date_from_filename(filename)
                    if os.path.exists(file_path):
                        local_date = self._extract_date_from_filename(os.path.basename(file_path))
                        if remote_date and local_date and remote_date <= local_date:
                            self.logger.info(f"No update needed for {filename}")
                            results.append((url, False))
                            continue

                    self.logger.info(f"Downloading {filename}")
                    await fm._download_with_progress(url, file_path)
                    results.append((url, True))
                except APIError as e:
                    self.logger.error(f"Error downloading {filename}: {str(e)}")
                    results.append((url, False))
        return results

    def _extract_date_from_filename(self, filename):
        try:
            if 'CENSUS_PUB_' in filename:
                date_str = filename.split('_')[2].split('.')[0]
                return datetime.strptime(date_str, '%Y%m%d')
            else:
                self.logger.warning(f"Unexpected filename format: {filename}")
                return None
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None