import os
import json
import asyncio
import aiohttp
import aiofiles
from datetime import datetime
from src.error_handler import FileError, APIError

class FileManager:
    def __init__(self, base_dir, max_concurrent_downloads=5):
        self.base_dir = base_dir
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
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

    async def _download_with_progress(self, url, file_path):
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                start_time = datetime.now()

                filename = os.path.basename(file_path)
                print(f"{filename}: Starting download")

                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
                        downloaded_size += len(chunk)
                        elapsed_time = (datetime.now() - start_time).total_seconds()
                        if elapsed_time > 0:
                            speed = downloaded_size / (1024 * 1024 * elapsed_time)
                            print(f"\r{filename}: {downloaded_size / (1024 * 1024):.1f}MiB [{elapsed_time:.0f}s, {speed:.2f}MiB/s]", end="", flush=True)

                print(f"\n{filename}: Download complete")

        except aiohttp.ClientError as e:
            raise APIError(f"Failed to download {url}: {str(e)}")