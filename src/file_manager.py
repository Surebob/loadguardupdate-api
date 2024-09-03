import logging
import os
import json
import asyncio
import aiohttp
import aiofiles
from datetime import datetime
from src.error_handler import FileError, APIError
from ftplib import FTP
from urllib.parse import urlparse

class FileManager:
    def __init__(self, base_dir, max_concurrent_downloads=5):
        self.base_dir = base_dir
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.session = None
        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=3600)  # Increased timeout
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def download_file(self, url, local_path):
        parsed_url = urlparse(url)
        if parsed_url.scheme == 'ftp':
            await self._download_ftp(parsed_url, local_path)
        else:
            await self._download_with_progress(url, local_path)

    async def _download_with_progress(self, url, file_path):
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                start_time = datetime.now()

                filename = os.path.basename(file_path)
                print(f"{filename}: Starting download")

                chunk_size = 8 * 1024 * 1024  # 8 MB chunks

                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        await f.write(chunk)
                        downloaded_size += len(chunk)
                        elapsed_time = (datetime.now() - start_time).total_seconds()
                        if elapsed_time > 0:
                            speed = downloaded_size / (1024 * 1024 * elapsed_time)
                            print(f"\r{filename}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.0f}s, {speed:.2f}MiB/s]", end="", flush=True)

                print(f"\n{filename}: Download complete")

        except asyncio.TimeoutError:
            print(f"\n{filename}: Download timed out. The download is taking longer than expected.")
            raise APIError(f"Timeout error while downloading {url}")
        except aiohttp.ClientError as e:
            print(f"\n{filename}: Download failed")
            raise APIError(f"Failed to download {url}: {str(e)}")
