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
import time

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
            latest_file = await self._find_latest_sms_file()
            if not latest_file:
                self.logger.warning("No available SMS file found on server")
                return False

            dataset_name = 'SMS'
            local_dir = os.path.join(self.base_dir, dataset_name)
            os.makedirs(local_dir, exist_ok=True)

            local_files = [f for f in os.listdir(local_dir) if f.endswith('.zip')]
            if not local_files:
<<<<<<< HEAD
                self.logger.info("No local SMS file found")
            elif latest_file == max(local_files, key=lambda f: self._extract_date_from_filename(f)):
                self.logger.info(f"No update needed for SMS file")
                return False

            self.logger.info(f"Latest SMS file found on server: {latest_file}")
            for old_file in local_files:
                os.remove(os.path.join(local_dir, old_file))
=======
                self.logger.info("Local SMS file not found")
            elif latest_file == max(local_files, key=lambda f: self._extract_date_from_filename(f)):
                self.logger.info(f"No update needed for SMS file")
                return False
>>>>>>> b81253eb88dee5ce54c2e351a1c576488ca17f6d

            self.logger.info(f"Latest SMS file found on server: {latest_file}")
            url = urljoin(self.base_url, latest_file)
            local_path = os.path.join(local_dir, latest_file)
            self.logger.info(f"Downloading {latest_file}")
<<<<<<< HEAD
            success = await self._download_with_progress(url, local_path)
            if not success:
                self.logger.error(f"Failed to download {latest_file}")
                return False
            return True

    async def _download_with_progress(self, url, local_path):
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return False
=======
            await self._download_with_progress(url, local_path)
            return True

    async def _download_with_progress(self, url, local_path):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
>>>>>>> b81253eb88dee5ce54c2e351a1c576488ca17f6d
                total_size = int(response.headers.get('Content-Length', 0))
                block_size = 1024 * 1024  # 1 MB
                downloaded_size = 0
                start_time = time.time()
                
                with open(local_path, 'wb') as f:
                    async for data in response.content.iter_chunked(block_size):
                        size = len(data)
                        downloaded_size += size
                        f.write(data)
                        
<<<<<<< HEAD
                        elapsed_time = max(time.time() - start_time, 0.1)  # Avoid division by zero
                        speed = downloaded_size / (1024 * 1024 * elapsed_time)
                        progress = f"INFO - {os.path.basename(local_path)}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.1f}s, {speed:.2f}MiB/s]"
                        print(f"\r{progress}", end="", flush=True)
                
                print()  # New line after download completes
            self.logger.info(f"Successfully downloaded {os.path.basename(local_path)}")
            return True
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return False
=======
                        elapsed_time = time.time() - start_time
                        speed = downloaded_size / (1024 * 1024 * elapsed_time)
                        progress = f"{os.path.basename(local_path)}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.0f}s, {speed:.2f}MiB/s]"
                        print(f"\r{progress}", end="", flush=True)
                
                print()  # New line after download completes
                if total_size != 0 and downloaded_size != total_size:
                    self.logger.error("ERROR, something went wrong")
                else:
                    self.logger.info(f"Successfully downloaded {url}")
>>>>>>> b81253eb88dee5ce54c2e351a1c576488ca17f6d

    async def _find_latest_sms_file(self):
        current_date = datetime.now()
        tasks = []
        
        for i in range(6):  # Try current month and five months back
            date = current_date - timedelta(days=30*i)
            filename = f"SMS_AB_PassProperty_{date.strftime('%Y%b')}.zip"
            url = urljoin(self.base_url, filename)
            tasks.append(self._check_and_download_file(url, filename))

        results = await asyncio.gather(*tasks)
        valid_files = [result for result in results if result]
        
        if valid_files:
            latest_file = max(valid_files, key=lambda f: self._extract_date_from_filename(f))
            return latest_file
        else:
            return None

    async def _check_and_download_file(self, url, filename):
        try:
            async with self.session.get(url, allow_redirects=True, timeout=30) as response:
                if response.status == 200:
                    content_start = await response.content.read(4)
                    is_zip = content_start == b'PK\x03\x04'
                    
                    if is_zip:
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
                            temp_file.write(await response.read())
<<<<<<< HEAD
=======
                            temp_file_path = temp_file.name
>>>>>>> b81253eb88dee5ce54c2e351a1c576488ca17f6d
                        return filename
            return None
        except Exception as e:
            self.logger.error(f"Error checking URL {url}: {str(e)}")
            return None

    def _extract_date_from_filename(self, filename):
        try:
            date_str = filename.split('_')[-1].split('.')[0]
            return datetime.strptime(date_str, '%Y%b')
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Error extracting date from filename {filename}: {str(e)}")
            return None
<<<<<<< HEAD

    async def test_connection(self):
        test_url = self.base_url
        try:
            async with self.session.get(test_url, timeout=30) as response:
                if response.status != 200:
                    self.logger.warning(f"Connection test to {test_url} returned status code: {response.status}")
        except Exception as e:
            self.logger.error(f"Error testing connection to {test_url}: {str(e)}")
=======
>>>>>>> b81253eb88dee5ce54c2e351a1c576488ca17f6d
