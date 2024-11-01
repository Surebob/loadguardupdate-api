import os
import logging
import asyncio
import aiofiles
from zipfile import ZipFile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class ZipProcessor:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self.executor = ThreadPoolExecutor(max_workers=3)  # Limit concurrent ZIP operations

    async def process_all_zips(self):
        """Process all ZIP files in their respective directories"""
        any_processed = False
        dir_types = ["FTP_Crash", "FTP_Inspection", "FTP_Violation", "SMS"]

        # Process ZIP files concurrently
        tasks = []
        for dir_type in dir_types:
            dir_path = os.path.join(self.base_dir, dir_type)
            if not os.path.exists(dir_path):
                continue

            # Create Extracted subdirectory
            extract_dir = os.path.join(dir_path, 'Extracted')
            os.makedirs(extract_dir, exist_ok=True)

            # Find ZIP files in directory
            zip_files = [f for f in os.listdir(dir_path) if f.endswith('.zip')]
            if not zip_files:
                continue

            for zip_file in zip_files:
                # Create task for each ZIP file
                task = asyncio.create_task(self.process_zip(dir_type, zip_file, extract_dir))
                tasks.append(task)

        # Wait for all ZIP processing tasks to complete
        if tasks:
            results = await asyncio.gather(*tasks)
            any_processed = any(results)

        return any_processed

    async def process_zip(self, dir_type, filename, extract_dir):
        """Process a single ZIP file"""
        try:
            self.logger.info(f"Processing {filename}")
            zip_path = os.path.join(self.base_dir, dir_type, filename)

            # Extract date from filename
            if dir_type == "SMS":
                date = filename.split('_')[-1].split('.')[0]
            else:
                date = filename.split('_')[1].split('.')[0]

            # Determine target file name
            target_file = {
                "FTP_Crash": f"{date}_Crash.txt",
                "FTP_Inspection": f"{date}_Inspection.txt",
                "FTP_Violation": f"{date}_Violation.txt",
                "SMS": f"SMS_AB_PassProperty_{date}.txt"
            }[dir_type]

            # Run ZIP extraction in thread pool
            def extract_file():
                with ZipFile(zip_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    target_lower = target_file.lower()
                    for file in file_list:
                        if file.lower() == target_lower:
                            zip_ref.extract(file, extract_dir)
                            extracted_path = os.path.join(extract_dir, file)
                            final_path = os.path.join(extract_dir, target_file)
                            if extracted_path != final_path:
                                os.rename(extracted_path, final_path)
                            return True
                return False

            # Run the CPU-intensive ZIP operation in a thread pool
            extracted = await asyncio.get_event_loop().run_in_executor(
                self.executor, 
                extract_file
            )

            if extracted:
                self.logger.info(f"Extracted {target_file} to {extract_dir}")
                return True
            else:
                self.logger.error(f"Target file {target_file} not found in {filename}")
                return False

        except Exception as e:
            self.logger.error(f"Error processing {filename}: {str(e)}")
            return False

    def __del__(self):
        self.executor.shutdown(wait=False)
