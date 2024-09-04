import os
import json
import asyncio
import aiohttp
import aiofiles
from datetime import datetime
from src.error_handler import FileError, APIError
from ftplib import FTP, error_perm
from urllib.parse import urlparse

class FileManager:
    def __init__(self, base_dir, max_concurrent_downloads=5):
        self.base_dir = base_dir
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.session = None

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
                    return json.loads(await f.read())
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
                    print(f"\n{os.path.basename(local_path)}: Download failed. Retrying in {retry_delay} seconds... ({max_retries - attempt - 1} attempts left)")
                    await asyncio.sleep(retry_delay)
                else:
                    print(f"\n{os.path.basename(local_path)}: Download failed after {max_retries} attempts: {str(e)}")
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
                print(f"{filename}: Starting download")

                def chunk_callback(data):
                    nonlocal downloaded_size
                    size = len(data)
                    downloaded_size += size
                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    if elapsed_time > 0:
                        speed = downloaded_size / (1024 * 1024 * elapsed_time)
                        print(f"\r{filename}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.0f}s, {speed:.2f}MiB/s]", end="", flush=True)
                    return data

                with open(local_path, 'wb') as f:
                    ftp.retrbinary(f'RETR {parsed_url.path}', lambda data: f.write(chunk_callback(data)), blocksize=8192)

        await asyncio.to_thread(ftp_download)
        print(f"\n{os.path.basename(local_path)}: Download complete")

    async def _download_with_progress(self, url, file_path):
        timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=3600)  # Increased timeout
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded_size = 0
                    start_time = datetime.now()

                    filename = os.path.basename(file_path)
                    print(f"{filename}: Starting download")

                    chunk_size = 8 * 1024 * 1024  # 8 MB chunks

                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            if chunk:  # Check if chunk is not empty
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
        except Exception as e:
            print(f"\n{filename}: Unexpected error during download")
            raise APIError(f"Unexpected error while downloading {url}: {str(e)}")