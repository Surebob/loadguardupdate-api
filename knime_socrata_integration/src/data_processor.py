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

    def process_dataset(self, dataset_id, category, last_check_time):
        try:
            metadata = self.api.get_dataset_metadata(dataset_id)
            last_update = datetime.fromtimestamp(metadata['rowsUpdatedAt'])
            
            if last_update > last_check_time:
                data = self.api.download_dataset(dataset_id)
                file_path = self.file_manager.save_dataset(dataset_id, category, data)
                logging.info(f"Updated file for {category}: {file_path}")
                return True
            else:
                logging.info(f"No updates for dataset {dataset_id} in category {category}")
                return False
        except Exception as e:
            raise ProcessingError(f"Error processing dataset {dataset_id} in category {category}: {str(e)}")