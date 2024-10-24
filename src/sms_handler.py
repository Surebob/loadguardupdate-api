# src/sms_handler.py

import os
import logging
import aiohttp, aiofiles
import asyncio
from datetime import datetime, timedelta
from config.settings import SMS_BASE_URL, DATA_DIR
from src.utils import ProgressBar
from src.error_handler import APIError

class SMSHandler:
    def __init__(self, session):
        self.base_url = SMS_BASE_URL
        self.base_dir = os.path.join(DATA_DIR, 'SMS')  # Will create if doesn't exist
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = session

    async def download_latest_sms_file(self):
        # Create SMS directory if it doesn't exist
        os.makedirs(self.base_dir, exist_ok=True)

        # Find the latest available file
        latest_file = await self.find_latest_available_file()
        if not latest_file:
            self.logger.warning("No available SMS file found on server")
            return False

        # Check if we already have this file
        local_files = [f for f in os.listdir(self.base_dir) if f.endswith('.zip')]
        
        if local_files:
            latest_local_file = max(local_files, key=lambda x: self.extract_date_from_filename(x))
            if latest_local_file == latest_file:
                self.logger.info("Already have the latest SMS file")
                return False

        # Remove old files before downloading new one
        for old_file in local_files:
            old_file_path = os.path.join(self.base_dir, old_file)
            os.remove(old_file_path)
            self.logger.info(f"Removed old file: {old_file}")

        # Download the latest file
        url = f"{self.base_url}{latest_file}"
        local_path = os.path.join(self.base_dir, latest_file)
        self.logger.info(f"Downloading {latest_file} from {url}")
        await self.download_file(url, local_path)
        self.logger.info(f"Downloaded SMS file: {latest_file}")
        return True

    async def find_latest_available_file(self):
        current_date = datetime.utcnow()
        available_files = []

        # Check files from 2 months in the past to 2 months in the future
        for i in range(-2, 3):  # -2, -1, 0, 1, 2
            check_date = current_date + timedelta(days=30 * i)
            filename = f"SMS_AB_PassProperty_{check_date.strftime('%Y%b')}.zip"
            url = f"{self.base_url}{filename}"
            
            # Add debug logging
            self.logger.debug(f"Checking URL: {url}")
            
            # Only check if file exists, don't download
            exists = await self.file_exists(url)
            self.logger.debug(f"File {filename} exists: {exists}")
            
            if exists:
                self.logger.info(f"Found available file: {filename}")
                available_files.append(filename)

        if not available_files:
            # Log which dates were checked
            self.logger.warning("No files found for the following dates:")
            for i in range(-2, 3):
                check_date = current_date + timedelta(days=30 * i)
                self.logger.warning(f"  - {check_date.strftime('%Y%b')}")
            return None

        # Return the filename with the latest date
        return max(available_files, key=lambda x: self.extract_date_from_filename(x))

    async def file_exists(self, url):
        try:
            # Use GET request instead of HEAD, but only read a small amount
            async with self.session.get(url) as response:
                self.logger.debug(f"GET request to {url} returned status: {response.status}")
                if response.status == 200:
                    # Read just a small chunk to verify it's a ZIP file
                    chunk = await response.content.read(8192)  # Read first 8KB
                    # ZIP files start with PK magic number (PK\x03\x04)
                    if chunk.startswith(b'PK'):
                        self.logger.debug(f"File exists and appears to be a valid ZIP: {url}")
                        return True
                    self.logger.debug("Response doesn't appear to be a ZIP file")
                    return False
                return False
        except Exception as e:
            self.logger.debug(f"Error checking URL {url}: {str(e)}")
            return False

    def extract_date_from_filename(self, filename):
        try:
            date_str = filename.split('_')[-1].split('.')[0]
            return datetime.strptime(date_str, '%Y%b')
        except Exception as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None

    async def download_file(self, url, local_path):
        progress = ProgressBar(f"Downloading {os.path.basename(local_path)}")
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                # Verify we're not getting an HTML error page
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type:
                    raise APIError(f"Received HTML instead of ZIP file from {url}")
                
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
            if os.path.exists(local_path):
                os.remove(local_path)
            raise APIError(f"Failed to download {url}: {str(e)}")
