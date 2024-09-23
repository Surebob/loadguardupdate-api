import os
import json
import time
import asyncio
import aiohttp
import aiofiles
from datetime import datetime
from src.error_handler import FileError, APIError
from ftplib import FTP, error_perm
from urllib.parse import urlparse
import logging

class FileManager:
    def __init__(self, base_dir, max_concurrent_downloads=5):
        self.base_dir = base_dir
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.session = None
        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=3600)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def read_metadata(self, dataset_name):
        try:
            metadata_file = os.path.join(self.base_dir, dataset_name, f"{dataset_name}_metadata.json")
            if os.path.exists(metadata_file):
                async with aiofiles.open(metadata_file, 'r') as f:
                    content = await f.read()
                    return json.loads(content)
            return None
        except Exception as e:
            raise FileError(f"Failed to read metadata for dataset {dataset_name}: {str(e)}")

    async def save_metadata(self, dataset_name, metadata):
        try:
            metadata_file = os.path.join(self.base_dir, dataset_name, f"{dataset_name}_metadata.json")
            async with aiofiles.open(metadata_file, 'w') as f:
                await f.write(json.dumps(metadata, indent=2))
        except Exception as e:
            raise FileError(f"Failed to save metadata for dataset {dataset_name}: {str(e)}")

    async def download_file(self, url, local_path):
        parsed_url = urlparse(url)
        if parsed_url.scheme == 'ftp':
            await self._download_ftp(parsed_url, local_path)
        else:
            await self._download_with_progress(url, local_path)

    async def _download_ftp(self, parsed_url, local_path):
        max_retries = 3
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                await self._download_ftp_attempt(parsed_url, local_path)
                return
            except (ConnectionResetError, error_perm) as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"{os.path.basename(local_path)}: Download failed. Retrying in {retry_delay} seconds... ({max_retries - attempt - 1} attempts left)")
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(f"{os.path.basename(local_path)}: Download failed after {max_retries} attempts: {str(e)}")
                    raise APIError(f"Failed to download {parsed_url.geturl()}: {str(e)}")

    async def _download_ftp_attempt(self, parsed_url, local_path):
        def ftp_download():
            with FTP(parsed_url.hostname) as ftp:
                ftp.login()
                ftp.set_pasv(True)  # Use passive mode
                total_size = ftp.size(parsed_url.path)
                downloaded_size = 0
                start_time = datetime.now()

                filename = os.path.basename(local_path)
                self.logger.info(f"{filename}: Starting download")

                with open(local_path, 'wb') as f:
                    def callback(data):
                        nonlocal downloaded_size
                        size = len(data)
                        downloaded_size += size
                        elapsed_time = (datetime.now() - start_time).total_seconds()
                        if elapsed_time > 0:
                            speed = downloaded_size / (1024 * 1024 * elapsed_time)
                            progress = f"{filename}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.0f}s, {speed:.2f}MiB/s]"
                            self.logger.info(progress)
                        f.write(data)

                    ftp.retrbinary(f'RETR {parsed_url.path}', callback, blocksize=8192)

        await asyncio.to_thread(ftp_download)
        self.logger.info(f"{os.path.basename(local_path)}: Download complete")

    async def _download_with_progress(self, url, local_path):
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return False

                total_size = int(response.headers.get('Content-Length', 0))
                block_size = 1024 * 1024  # 1 MB
                downloaded_size = 0
                start_time = time.time()
                last_log_time = start_time
                log_interval = 2  # Log every 2 seconds

                with open(local_path, 'wb') as f:
                    async for data in response.content.iter_chunked(block_size):
                        size = len(data)
                        downloaded_size += size
                        f.write(data)

                        # Log progress to the console every `log_interval` seconds
                        current_time = time.time()
                        if current_time - last_log_time >= log_interval:
                            elapsed_time = max(current_time - start_time, 0.1)  # Avoid division by zero
                            speed = downloaded_size / (1024 * 1024 * elapsed_time)
                            progress = f"{os.path.basename(local_path)}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.1f}s, {speed:.2f}MiB/s]"
                            print(f"\r{progress}", end='', flush=True)  # Overwrite the same line
                            last_log_time = current_time
                    
                # Print a newline to end the overwriting after download is complete
                print()  
                self.logger.info(f"Successfully downloaded {os.path.basename(local_path)}")
                return True
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return False


        except aiohttp.ClientError as e:
            self.logger.error(f"Failed to download {url}: {str(e)}")
            raise APIError(f"Failed to download {url}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error during download of {url}: {str(e)}")
            raise APIError(f"Unexpected error while downloading {url}: {str(e)}")
