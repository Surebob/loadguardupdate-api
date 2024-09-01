import os
from datetime import datetime
from src.socrata_api import SocrataAPI
from src.file_manager import FileManager
from src.error_handler import ProcessingError
import logging

class DataProcessor:
    def __init__(self, api: SocrataAPI, file_manager: FileManager):
        self.api = api
        self.file_manager = file_manager

    def process_dataset(self, dataset_name):
        try:
            last_update = self.api.check_dataset_update(dataset_name)
            file_path = os.path.join(self.file_manager.base_dir, dataset_name, f"{dataset_name}.csv")
            
            if not os.path.exists(file_path) or last_update > datetime.fromtimestamp(os.path.getmtime(file_path)):
                data = self.api.download_dataset(dataset_name)
                saved_path = self.file_manager.save_dataset(dataset_name, data)
                logging.info(f"Updated file for {dataset_name}: {saved_path}")
                return True
            else:
                logging.info(f"No updates for dataset {dataset_name}")
                return False
        except Exception as e:
            raise ProcessingError(f"Error processing dataset {dataset_name}: {str(e)}")