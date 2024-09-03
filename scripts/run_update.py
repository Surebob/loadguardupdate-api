import sys
import os
import asyncio
import schedule
import time
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.socrata_api import SocrataAPI
from src.knime_runner import KNIMERunner
from src.zip_file_handler import ZipFileHandler
from src.dropbox_handler import DropboxHandler
from config.settings import KNIME_EXECUTABLE, DATA_DIR, SCHEDULE_TIME, TIMEZONE
from src.error_handler import KNIMEError, APIError
from config.logging_config import configure_logging
import logging

configure_logging()
logger = logging.getLogger(__name__)

async def job():
    logger.info("Running update job")
    socrata_api = SocrataAPI(DATA_DIR)
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)
    zip_handler = ZipFileHandler(DATA_DIR)
    dropbox_handler = DropboxHandler(DATA_DIR)

    updated_datasets = []

    # Process Socrata datasets
    if await socrata_api.update_and_download_datasets():
        updated_datasets.extend(socrata_api.datasets.keys())

    # Process ZIP files
    zip_results = await zip_handler.download_all()
    updated_datasets.extend([os.path.splitext(os.path.basename(url))[0] for url, result in zip_results if result])

    # Process Dropbox datasets
    dropbox_results = await dropbox_handler.download_datasets()
    updated_datasets.extend([os.path.splitext(os.path.basename(url.split('?')[0]))[0] for url, result in dropbox_results if result])

    if updated_datasets:
        try:
            logger.info("Running KNIME workflow")
            output = await knime_runner.run_workflow()  # Remove the argument here
            logger.info("KNIME workflow executed successfully")
            logger.debug(f"KNIME output: {output}")
        except KNIMEError as e:
            logger.error(f"Error running KNIME workflow: {str(e)}")
    else:
        logger.info("No datasets were updated, skipping KNIME workflow execution")

def run_job():
    asyncio.run(job())

def main():
    logger.info("Starting the update process")
    
    # Run the job immediately
    run_job()
    
    # Schedule the job to run at the specified time
    schedule.every().day.at(SCHEDULE_TIME).do(run_job)
    
    logger.info(f"Scheduled to run daily at {SCHEDULE_TIME} {TIMEZONE}")
    
    while True:
        # Calculate time until next run
        now = datetime.now(TIMEZONE)
        next_run = schedule.next_run()
        if next_run:
            # Convert next_run to timezone-aware
            next_run = TIMEZONE.localize(next_run.replace(tzinfo=None))
            sleep_seconds = (next_run - now).total_seconds()
            sleep_seconds = max(0, min(sleep_seconds, 3600))  # Sleep at most 1 hour
            logger.info(f"Sleeping for {sleep_seconds / 60:.2f} minutes until next check")
            time.sleep(sleep_seconds)
        else:
            time.sleep(3600)  # If no next run, sleep for 1 hour
        
        schedule.run_pending()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Update process stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)