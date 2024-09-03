import os
from datetime import datetime, timedelta
import logging
import aiohttp
import ssl
import asyncio
from urllib.parse import urljoin
from src.file_downloader import FileDownloader
import traceback
import tempfile

class SMSUpdater:
    def __init__(self, base_dir, base_url):
        self.base_dir = base_dir
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        self.file_downloader = FileDownloader(base_dir)
        self.session = None

    async def __aenter__(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"},
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def check_and_download_latest(self):
        async with self:
            self.logger.info("Starting check_and_download_latest")
            await self.test_connection()
            latest_file = await self._find_latest_sms_file()
            if not latest_file:
                self.logger.warning("No available SMS file found")
                return False

            dataset_name = 'SMS'
            local_dir = os.path.join(self.base_dir, dataset_name)
            os.makedirs(local_dir, exist_ok=True)

            local_files = [f for f in os.listdir(local_dir) if f.endswith('.zip')]
            if local_files:
                latest_local_file = max(local_files, key=lambda f: self._extract_date_from_filename(f))
                if latest_file == latest_local_file:
                    self.logger.info(f"No update needed for SMS file")
                    return False

            self.logger.info(f"Newer version available for SMS: {latest_file}")
            for old_file in local_files:
                os.remove(os.path.join(local_dir, old_file))
                self.logger.info(f"Removed old file: {old_file}")

            url = urljoin(self.base_url, latest_file)
            local_path = os.path.join(local_dir, latest_file)
            self.logger.info(f"Downloading {latest_file} to {local_path}")
            success = await self.file_downloader.check_and_download(url, local_path)
            if not success:
                self.logger.error(f"Failed to download {latest_file}")
                return False
            return True

    async def _find_latest_sms_file(self):
        self.logger.info("Starting _find_latest_sms_file")
        current_date = datetime.now()
        tasks = []
        
        for i in range(6):  # Try current month and five months back
            date = current_date - timedelta(days=30*i)
            filename = f"SMS_AB_PassProperty_{date.strftime('%Y%b')}.zip"
            url = urljoin(self.base_url, filename)
            tasks.append(self._check_and_download_file(url, filename))

        self.logger.info(f"Created {len(tasks)} tasks for file checking")
        results = await asyncio.gather(*tasks)
        self.logger.info(f"Gathered results: {results}")
        valid_files = [result for result in results if result]
        self.logger.info(f"Valid files: {valid_files}")
        
        if valid_files:
            latest_file = max(valid_files, key=lambda f: self._extract_date_from_filename(f))
            self.logger.info(f"Latest SMS file found: {latest_file}")
            return latest_file
        else:
            self.logger.warning("No valid SMS files found")
            return None

    async def _check_and_download_file(self, url, filename):
        self.logger.info(f"Checking for SMS file: {url}")
        try:
            async with self.session.get(url, allow_redirects=True, timeout=30) as response:
                self.logger.info(f"Response status for {url}: {response.status}")
                self.logger.info(f"Response headers: {response.headers}")
                if response.status == 200:
                    # Check if it's actually a ZIP file
                    content_type = response.headers.get('Content-Type', '')
                    content_disposition = response.headers.get('Content-Disposition', '')
                    self.logger.info(f"Content-Type: {content_type}, Content-Disposition: {content_disposition}")
                    
                    # Read the first few bytes to check if it's a ZIP file
                    content_start = await response.content.read(4)
                    is_zip = content_start == b'PK\x03\x04'
                    self.logger.info(f"First 4 bytes: {content_start}, Is ZIP: {is_zip}")
                    
                    if is_zip:
                        # It's a ZIP file, download it to a temporary file
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
                            temp_file.write(await response.read())
                            temp_file_path = temp_file.name
                        
                        self.logger.info(f"Found valid SMS file: {filename}")
                        return filename
                    else:
                        self.logger.info(f"File at {url} is not a ZIP file")
                else:
                    self.logger.info(f"SMS file not found: {filename}")
            return None
        except Exception as e:
            self.logger.error(f"Error checking URL {url}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None

    def _extract_date_from_filename(self, filename):
        try:
            date_str = filename.split('_')[-1].split('.')[0]
            return datetime.strptime(date_str, '%Y%b')
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None

    async def test_connection(self):
        test_url = self.base_url
        self.logger.info(f"Testing connection to {test_url}")
        try:
            async with self.session.get(test_url, timeout=30) as response:
                self.logger.info(f"Connection test to {test_url} returned status code: {response.status}")
                self.logger.debug(f"Response headers: {response.headers}")
                content = await response.text()
                self.logger.debug(f"Response content (first 500 characters): {content[:500]}")
        except Exception as e:
            self.logger.error(f"Error testing connection to {test_url}: {str(e)}")
            self.logger.debug(f"Connection test error details: {traceback.format_exc()}")
