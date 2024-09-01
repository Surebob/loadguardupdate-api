import schedule
import time
from datetime import datetime, timedelta
from src.socrata_api import SocrataAPI
from src.data_processor import DataProcessor
from src.file_manager import FileManager
from src.knime_runner import KNIMERunner
from config.settings import KNIME_EXECUTABLE, DATA_DIR, CHECK_INTERVAL_HOURS
from src.error_handler import KNIMEError
import logging

def main():
    api = SocrataAPI(DATA_DIR)
    file_manager = FileManager(DATA_DIR)
    processor = DataProcessor(api, file_manager)
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)

    def job():
        updated_datasets = []

        for dataset_name in api.datasets:
            try:
                if processor.process_dataset(dataset_name):
                    updated_datasets.append(dataset_name)
            except Exception as e:
                logging.error(f"Error processing dataset {dataset_name}: {str(e)}")

        if updated_datasets:
            api.download_dropbox_datasets()
            try:
                output = knime_runner.run_workflow({"updated_datasets": ",".join(updated_datasets)})
                logging.info("KNIME workflow executed successfully")
                logging.info(f"KNIME output: {output}")
            except KNIMEError as e:
                logging.error(f"Error running KNIME workflow: {str(e)}")

    schedule.every(CHECK_INTERVAL_HOURS).hours.do(job)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()