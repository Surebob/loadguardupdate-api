import schedule
import time
from datetime import datetime, timedelta
from src.socrata_api import SocrataAPI
from src.data_processor import DataProcessor
from src.file_manager import FileManager
from src.knime_runner import KNIMERunner
from config.settings import SOCRATA_BASE_URL, KNIME_EXECUTABLE, KNIME_WORKFLOW_DIR, DATA_DIR, DATASETS, CHECK_INTERVAL_HOURS
from src.error_handler import KNIMEError
import logging

def main():
    api = SocrataAPI(SOCRATA_BASE_URL)
    file_manager = FileManager(DATA_DIR)
    processor = DataProcessor(api, file_manager)
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)

    def job():
        last_check_time = datetime.now() - timedelta(hours=CHECK_INTERVAL_HOURS)
        updated_datasets = []

        for dataset in DATASETS:
            try:
                if processor.process_dataset(dataset['id'], dataset['category'], last_check_time):
                    updated_datasets.append(dataset['id'])
            except Exception as e:
                logging.error(f"Error processing dataset {dataset['id']}: {str(e)}")

        if updated_datasets:
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