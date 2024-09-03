import os
from urllib.parse import urlparse
from datetime import datetime
import logging
import aiohttp
import asyncio
import aiofiles  # Add this import
from ftplib import FTP
from src.error_handler import APIError
from src.file_manager import FileManager
from config.settings import DROPBOX_DATASETS

class FileDownloader:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logger = logging.getLogger(__name__)
        self.file_manager = FileManager(base_dir)

    async def check_and_download(self, url, local_path=None):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(local_path, mode='wb') as f:
                            await f.write(await response.read())
                        print(f"Successfully downloaded {url} to {local_path}")
                        return True
                    else:
                        print(f"Failed to download {url}. Status: {response.status}")
                        return False
        except Exception as e:
            print(f"Error downloading {url}: {str(e)}")
            return False

    async def _download_ftp(self, parsed_url, local_path):
        def ftp_download():
            with FTP(parsed_url.hostname) as ftp:
                ftp.login()
                total_size = ftp.size(parsed_url.path)
                downloaded_size = 0
                start_time = datetime.now()

                filename = os.path.basename(local_path)
                self.logger.info(f"Starting download of {filename}")

                def chunk_callback(data):
                    nonlocal downloaded_size
                    size = len(data)
                    downloaded_size += size
                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    if elapsed_time > 0:
                        speed = downloaded_size / (1024 * 1024 * elapsed_time)
                        progress = f"\r{downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.0f}s, {speed:.2f}MiB/s]"
                        print(progress, end="", flush=True)
                    return data

                with open(local_path, 'wb') as f:
                    ftp.retrbinary(f'RETR {parsed_url.path}', lambda data: f.write(chunk_callback(data)))

        await asyncio.to_thread(ftp_download)
        print()  # New line after download completes
        self.logger.info(f"Download complete: {os.path.basename(local_path)}")

    async def _get_remote_file_size(self, url):
        parsed_url = urlparse(url)
        if parsed_url.scheme == 'ftp':
            return await asyncio.to_thread(self._get_ftp_file_size, parsed_url)
        else:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url) as response:
                        return int(response.headers.get('Content-Length', 0))
            except Exception as e:
                self.logger.warning(f"Failed to get remote file size for {url}: {str(e)}")
                return None

    def _get_ftp_file_size(self, parsed_url):
        try:
            with FTP(parsed_url.hostname) as ftp:
                ftp.login()
                return ftp.size(parsed_url.path)
        except Exception as e:
            self.logger.warning(f"Failed to get FTP file size for {parsed_url.geturl()}: {str(e)}")
            return None
