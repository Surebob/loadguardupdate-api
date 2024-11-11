import os
import logging
import asyncio
import aiofiles
from zipfile import ZipFile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import shutil
import traceback

class ZipProcessor:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self.executor = ThreadPoolExecutor(max_workers=3)

    async def process_all_zips(self):
        """Process all ZIP files in their respective directories"""
        any_processed = False
        dir_types = ["FTP_Crash", "FTP_Inspection", "FTP_Violation", "SMS"]
        tasks = []

        # First ensure base directories exist with proper permissions
        for dir_type in dir_types:
            dir_path = os.path.join(self.base_dir, dir_type)
            self.logger.debug(f"Checking directory: {dir_path}")
            
            try:
                # Create base directory if it doesn't exist
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
                    self.logger.debug(f"Created base directory: {dir_path}")
            except Exception as e:
                self.logger.error(f"Could not create base directory {dir_path}: {str(e)}")
                continue

            # Create Extracted subdirectory
            extract_dir = os.path.join(dir_path, 'Extracted')
            self.logger.debug(f"Attempting to create/clear directory: {extract_dir}")
            
            try:
                # First try to create the directory if it doesn't exist
                if not os.path.exists(extract_dir):
                    os.makedirs(extract_dir)
                    self.logger.debug(f"Created extract directory: {extract_dir}")
                else:
                    # If it exists, try to clean it
                    try:
                        # Instead of removing the directory, just remove its contents
                        for item in os.listdir(extract_dir):
                            item_path = os.path.join(extract_dir, item)
                            try:
                                if os.path.isfile(item_path):
                                    os.unlink(item_path)
                                elif os.path.isdir(item_path):
                                    shutil.rmtree(item_path)
                            except Exception as e:
                                self.logger.error(f"Could not remove {item_path}: {str(e)}")
                    except Exception as e:
                        self.logger.error(f"Could not clean directory {extract_dir}: {str(e)}")
                        # Continue anyway as the directory exists
            except Exception as e:
                self.logger.error(f"Could not create/access directory {extract_dir}: {str(e)}")
                continue

            # Find ZIP files in directory
            try:
                zip_files = [f for f in os.listdir(dir_path) if f.endswith('.zip')]
                if zip_files:
                    self.logger.debug(f"Found ZIP files in {dir_path}: {zip_files}")
                    for zip_file in zip_files:
                        task = asyncio.create_task(self.process_zip(dir_type, zip_file, extract_dir))
                        tasks.append(task)
            except Exception as e:
                self.logger.error(f"Error processing directory {dir_path}: {str(e)}")
                continue

        # Process all tasks if any exist
        if tasks:
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Task failed with error: {str(result)}")
                        self.logger.error(f"Stack trace: {traceback.format_exc()}")
                    elif result:
                        any_processed = True
            except Exception as e:
                self.logger.error(f"Error processing ZIP files: {str(e)}")
                self.logger.error(f"Stack trace: {traceback.format_exc()}")

        return any_processed

    async def process_zip(self, dir_type, filename, extract_dir):
        """Process a single ZIP file"""
        try:
            self.logger.info(f"Processing {filename}")
            zip_path = os.path.join(self.base_dir, dir_type, filename)
            self.logger.debug(f"Full ZIP path: {zip_path}")

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

            def extract_file():
                try:
                    self.logger.debug(f"Opening ZIP file: {zip_path}")
                    with ZipFile(zip_path, 'r') as zip_ref:
                        file_list = zip_ref.namelist()
                        self.logger.debug(f"Files in ZIP: {file_list}")
                        target_lower = target_file.lower()
                        for file in file_list:
                            if file.lower() == target_lower:
                                self.logger.debug(f"Extracting {file} to {extract_dir}")
                                try:
                                    zip_ref.extract(file, extract_dir)
                                except PermissionError as pe:
                                    self.logger.error(f"Permission error extracting {file}: {str(pe)}")
                                    raise
                                
                                extracted_path = os.path.join(extract_dir, file)
                                final_path = os.path.join(extract_dir, target_file)
                                
                                # Rename if needed
                                if extracted_path != final_path:
                                    self.logger.debug(f"Renaming {extracted_path} to {final_path}")
                                    if os.path.exists(final_path):
                                        try:
                                            os.remove(final_path)
                                        except PermissionError as pe:
                                            self.logger.error(f"Permission error removing existing file {final_path}: {str(pe)}")
                                            raise
                                    try:
                                        os.rename(extracted_path, final_path)
                                    except PermissionError as pe:
                                        self.logger.error(f"Permission error renaming file: {str(pe)}")
                                        raise
                                return True
                    return False
                except Exception as e:
                    self.logger.error(f"Error in extract_file: {str(e)}")
                    self.logger.error(f"Stack trace: {traceback.format_exc()}")
                    raise

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
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            return False

    def __del__(self):
        self.executor.shutdown(wait=False)
