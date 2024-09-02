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
            current_metadata = self.api.check_dataset_update(dataset_name)
            current_update_time = datetime.fromtimestamp(current_metadata['rowsUpdatedAt'])
            
            saved_metadata = self.file_manager.read_metadata(dataset_name)
            
            needs_update = True
            if saved_metadata and 'rowsUpdatedAt' in saved_metadata:
                last_update_time = datetime.fromisoformat(saved_metadata['rowsUpdatedAt'])
                needs_update = current_update_time > last_update_time

            if needs_update:
                data = self.api.download_dataset(dataset_name)
                saved_path = self.file_manager.save_dataset(dataset_name, data)
                
                if saved_path:
                    # Save metadata only if the dataset was actually updated
                    self.file_manager.save_metadata(dataset_name, {
                        'rowsUpdatedAt': current_update_time.isoformat()
                    })
                    logging.info(f"Updated file for {dataset_name}: {saved_path}")
                    return True
                else:
                    logging.info(f"No changes in data for {dataset_name}")
                    return False
            else:
                logging.info(f"No updates for dataset {dataset_name}")
                return False
        except Exception as e:
            raise ProcessingError(f"Error processing dataset {dataset_name}: {str(e)}")