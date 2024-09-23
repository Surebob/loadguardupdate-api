# zip_file_handler.py

import os
import logging
import asyncio
from datetime import datetime
from urllib.parse import urlparse, urljoin
from ftplib import FTP
from src.error_handler import APIError
from src.file_manager import FileManager
from config.settings import ZIP_FILES
from dateutil.relativedelta import relativedelta
import re

class ZipFileHandler:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logger = logging.getLogger(__name__)
        self.zip_files = ZIP_FILES
        self.file_manager = FileManager(base_dir)

    async def download_all(self):
        results = []
        async with self.file_manager:
            for url in self.zip_files:
                parsed_url = urlparse(url)
                if parsed_url.scheme == 'ftp':
                    result = await self._handle_ftp(parsed_url)
                else:
                    result = await self._handle_http(url)
                results.append((url, result))
        return results

    async def _handle_ftp(self, parsed_url):
        remote_files = await self._list_ftp_files(parsed_url)
        if not remote_files:
            self.logger.warning(f"No files found in FTP directory: {parsed_url.geturl()}")
            return False

        updates = []
        for file_type in ['Crash', 'Inspection', 'Violation']:
            dataset_name = f'FTP_{file_type}'
            local_dir = os.path.join(self.base_dir, dataset_name)
            os.makedirs(local_dir, exist_ok=True)

            valid_remote_files = [f for f in remote_files if f.startswith(f'{file_type}_') and f.endswith('.zip')]
            if not valid_remote_files:
                self.logger.warning(f"No valid {file_type} files found in FTP directory")
                continue

            dated_files = [(f, self._extract_date_from_filename(f)) for f in valid_remote_files]
            dated_files = [(f, d) for f, d in dated_files if d is not None]

            if not dated_files:
                self.logger.warning(f"No valid dates could be extracted from {file_type} files")
                continue

            latest_remote_file, latest_remote_date = max(dated_files, key=lambda x: x[1])

            local_files = [f for f in os.listdir(local_dir) if f.endswith('.zip')]
            latest_local_date = None
            if local_files:
                dated_local_files = [(f, self._extract_date_from_filename(f)) for f in local_files]
                dated_local_files = [(f, d) for f, d in dated_local_files if d is not None]
                if dated_local_files:
                    _, latest_local_date = max(dated_local_files, key=lambda x: x[1])

            if latest_local_date and latest_remote_date <= latest_local_date:
                self.logger.info(f"No update needed for {dataset_name}")
                continue

            self.logger.info(f"Newer version available for {dataset_name}: {latest_remote_file}")
            for old_file in local_files:
                os.remove(os.path.join(local_dir, old_file))
                self.logger.info(f"Removed old file: {old_file}")

            await self._download_file(parsed_url, latest_remote_file, local_dir)
            updates.append(dataset_name)

        return len(updates) > 0

    async def _handle_http(self, url):
        # Skip base URLs without a file path
        if url.endswith('/'):
            self.logger.warning(f"Skipping directory URL: {url}")
            return False

        parsed_url = urlparse(url)
        remote_filename = os.path.basename(parsed_url.path)
        
        if not remote_filename:
            self.logger.warning(f"Empty or invalid filename detected for URL: {url}")
            return False

        dataset_name = self._get_dataset_name(parsed_url.path)
        local_dir = os.path.join(self.base_dir, dataset_name)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, remote_filename)
        self.logger.info(f"Downloading {remote_filename} to {local_path}")
        await self.file_manager.download_file(url, local_path)
        self.logger.info(f"Downloaded {remote_filename} to {dataset_name} directory")
        return True

    
    def _get_dataset_name(self, path):
        filename = os.path.basename(path)
        if 'SMS_AB_PassProperty_' in filename:
            return 'SMS'
        elif 'Crash_' in filename:
            return 'FTP_Crash'
        elif 'Inspection_' in filename:
            return 'FTP_Inspection'
        elif 'Violation_' in filename:
            return 'FTP_Violation'
        else:
            return os.path.splitext(filename)[0]

    async def _list_ftp_files(self, parsed_url):
        def ftp_list():
            with FTP(parsed_url.hostname) as ftp:
                ftp.login()
                ftp.cwd(parsed_url.path)
                return ftp.nlst()

        return await asyncio.to_thread(ftp_list)

    async def _download_file(self, parsed_url, filename, local_dir):
        remote_path = f"{parsed_url.path}/{filename}"
        local_path = os.path.join(local_dir, filename)
        full_url = parsed_url._replace(path=remote_path).geturl()
        try:
            await self.file_manager.download_file(full_url, local_path)
        except APIError as e:
            self.logger.error(f"Failed to download {filename}: {str(e)}")
            if os.path.exists(local_path):
                os.remove(local_path)
            raise

    def _extract_date_from_filename(self, filename):
        try:
            match = re.search(r'_(\d{4}[A-Za-z]{3})', filename)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%Y%b')
            else:
                self.logger.warning(f"Date not found in filename: {filename}")
                return None
        except Exception as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None
