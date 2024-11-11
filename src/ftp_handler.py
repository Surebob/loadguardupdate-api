# src/ftp_handler.py
import asyncio
import os
import logging
import re
from datetime import datetime
from ftplib import FTP, error_perm
from src.error_handler import APIError
from config.settings import FTP_URL, DATA_DIR
from src.utils import ProgressBar
import socket

class FTPHandler:
    def __init__(self):
        self.ftp_url = FTP_URL
        self.base_dir = DATA_DIR
        self.logger = logging.getLogger(self.__class__.__name__)

    async def download_ftp_files(self):
        updates = []
        for file_type in ['Crash', 'Inspection', 'Violation']:
            dataset_name = f'FTP_{file_type}'
            local_dir = os.path.join(self.base_dir, dataset_name)
            
            # Create directory with explicit error handling
            try:
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                    self.logger.debug(f"Created directory: {local_dir}")
                
                # Create Extracted subdirectory here
                extract_dir = os.path.join(local_dir, 'Extracted')
                if not os.path.exists(extract_dir):
                    os.makedirs(extract_dir)
                    self.logger.debug(f"Created Extracted directory: {extract_dir}")
            except PermissionError as pe:
                self.logger.error(f"Permission error creating directory {local_dir}: {str(pe)}")
                continue
            except Exception as e:
                self.logger.error(f"Error creating directory {local_dir}: {str(e)}")
                continue

            latest_remote_file = await self.find_latest_file(file_type)
            if not latest_remote_file:
                self.logger.info(f"No remote files found for {file_type}")
                continue

            latest_local_file = self.find_latest_local_file(local_dir, file_type)
            if latest_local_file:
                latest_local_date = self.extract_date_from_filename(latest_local_file)
                latest_remote_date = self.extract_date_from_filename(latest_remote_file)
                if latest_local_date and latest_remote_date and latest_remote_date <= latest_local_date:
                    self.logger.info(f"No update needed for {dataset_name}")
                    continue

            # Remove old files with error handling
            try:
                for old_file in os.listdir(local_dir):
                    old_file_path = os.path.join(local_dir, old_file)
                    if os.path.isfile(old_file_path):  # Only remove files, not directories
                        try:
                            os.remove(old_file_path)
                            self.logger.info(f"Removed old file: {old_file}")
                        except PermissionError as pe:
                            self.logger.error(f"Permission error removing file {old_file}: {str(pe)}")
                        except Exception as e:
                            self.logger.error(f"Error removing file {old_file}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error cleaning directory {local_dir}: {str(e)}")

            # Download the latest file
            try:
                await self.download_file(latest_remote_file, local_dir)
                updates.append(dataset_name)
                self.logger.info(f"Downloaded latest file {latest_remote_file} for {dataset_name}")
            except Exception as e:
                self.logger.error(f"Error downloading {latest_remote_file}: {str(e)}")

        return len(updates) > 0

    async def find_latest_file(self, file_type):
        def ftp_list():
            with FTP('ftp.senture.com') as ftp:
                ftp.login()
                files = ftp.nlst()
                pattern = re.compile(f"{file_type}_\\d{{4}}[A-Za-z]{{3}}\\.zip")
                valid_files = [f for f in files if pattern.match(f)]
                if valid_files:
                    latest_file = max(valid_files, key=lambda x: self.extract_date_from_filename(x))
                    return latest_file
                else:
                    return None

        return await asyncio.to_thread(ftp_list)

    def find_latest_local_file(self, local_dir, file_type):
        local_files = [f for f in os.listdir(local_dir) if f.startswith(f"{file_type}_") and f.endswith('.zip')]
        if local_files:
            latest_file = max(local_files, key=lambda x: self.extract_date_from_filename(x))
            return latest_file
        else:
            return None

    def extract_date_from_filename(self, filename):
        try:
            match = re.search(r'_(\d{4}[A-Za-z]{3})', filename)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%Y%b')
            else:
                return None
        except Exception as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None

    async def download_file(self, filename, local_dir):
        local_path = os.path.join(local_dir, filename)
        progress = ProgressBar(f"Downloading {filename}")

        def ftp_download():
            with FTP('ftp.senture.com') as ftp:
                ftp.set_pasv(True)
                ftp.login()
                total_size = 0
                progress.start()

                def callback(data):
                    nonlocal total_size
                    total_size += len(data)
                    progress.update(total_size)
                    return f.write(data)

                with open(local_path, 'wb') as f:
                    ftp.retrbinary(f"RETR {filename}", callback, blocksize=1024*1024)
                
                progress.finish()

        await asyncio.to_thread(ftp_download)
