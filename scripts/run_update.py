import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import schedule
import time
from datetime import datetime, timedelta
from src.socrata_api import SocrataAPI
from src.data_processor import DataProcessor
from src.file_manager import FileManager
from src.knime_runner import KNIMERunner
from src.zip_file_handler import ZipFileHandler
from config.settings import KNIME_EXECUTABLE, DATA_DIR, CHECK_INTERVAL_HOURS
from src.error_handler import KNIMEError
from config.logging_config import configure_logging
import logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def job():
    logger.info("Running update job")
    api = SocrataAPI(DATA_DIR)
    file_manager = FileManager(DATA_DIR)
    processor = DataProcessor(api, file_manager)
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)
    zip_handler = ZipFileHandler(DATA_DIR)

    updated_datasets = []

    # Process Socrata datasets
    for dataset_name in api.datasets:
        try:
            logger.info(f"Processing dataset: {dataset_name}")
            if processor.process_dataset(dataset_name):
                updated_datasets.append(dataset_name)
                logger.info(f"Dataset {dataset_name} was updated")
            else:
                logger.info(f"No updates for dataset {dataset_name}")
        except Exception as e:
            logger.error(f"Error processing dataset {dataset_name}: {str(e)}", exc_info=True)

    # Process ZIP files
    zip_files = [
        "ftp://ftp.senture.com/Inspection_2024Jul.zip",
        "https://ai.fmcsa.dot.gov/SMS/files/SMS_AB_PassProperty_2024Jun.zip"
    ]
    for zip_url in zip_files:
        try:
            if zip_handler.check_and_download(zip_url):
                updated_datasets.append(os.path.basename(zip_url))
                logger.info(f"ZIP file {os.path.basename(zip_url)} was updated")
            else:
                logger.info(f"No update needed for ZIP file {os.path.basename(zip_url)}")
        except Exception as e:
            logger.error(f"Error processing ZIP file {zip_url}: {str(e)}", exc_info=True)

    if updated_datasets:
        logger.info("Downloading Dropbox datasets")
        api.download_dropbox_datasets()
        try:
            logger.info("Running KNIME workflow")
            output = knime_runner.run_workflow({"updated_datasets": ",".join(updated_datasets)})
            logger.info("KNIME workflow executed successfully")
            logger.debug(f"KNIME output: {output}")
        except KNIMEError as e:
            logger.error(f"Error running KNIME workflow: {str(e)}", exc_info=True)
    else:
        logger.info("No datasets were updated, skipping KNIME workflow execution")

def main():
    logger.info("Starting the update process")
    
    # Run the job immediately
    job()
    
    # Then schedule it
    schedule.every(CHECK_INTERVAL_HOURS).hours.do(job)
    logger.info(f"Scheduled job to run every {CHECK_INTERVAL_HOURS} hours")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)