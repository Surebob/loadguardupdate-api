import sys
import os
import asyncio
import schedule
import time
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.socrata_api import SocrataAPI
from main_scripts.knime_runner import KNIMERunner
from src.zip_file_handler import ZipFileHandler
from src.dropbox_handler import DropboxHandler
from config.settings import (KNIME_EXECUTABLE, DATA_DIR, TIMEZONE,
                             DATASET_UPDATE_INTERVAL, KNIME_WORKFLOW_TIME)
from src.error_handler import KNIMEError, APIError
from config.logging_config import configure_logging
import logging

configure_logging()
logger = logging.getLogger(__name__)

async def update_datasets():
    logger.info("Checking for dataset updates")
    socrata_api = SocrataAPI(DATA_DIR)
    zip_handler = ZipFileHandler(DATA_DIR)
    dropbox_handler = DropboxHandler(DATA_DIR)

    updated = False

    # Process Socrata datasets
    if await socrata_api.update_and_download_datasets():
        updated = True

    # Process ZIP files
    zip_results = await zip_handler.download_all()
    if any(result for _, result in zip_results):
        updated = True

    # Process Dropbox datasets
    dropbox_results = await dropbox_handler.download_datasets()
    if any(result for _, result in dropbox_results):
        updated = True

    if updated:
        logger.info("Datasets have been updated")
    else:
        logger.info("No updates found for datasets")

async def run_knime_workflow():
    logger.info("Running KNIME workflow")
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)
    try:
        await knime_runner.run_workflow()
        logger.info("KNIME workflow executed successfully")
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")

def run_dataset_update():
    asyncio.run(update_datasets())

def run_knime_job():
    asyncio.run(run_knime_workflow())

def main():
    logger.info("Starting the update process")

    # Schedule dataset updates
    schedule.every(DATASET_UPDATE_INTERVAL).minutes.do(run_dataset_update)

    # Schedule KNIME workflow
    schedule.every().day.at(KNIME_WORKFLOW_TIME).do(run_knime_job)

    logger.info(f"Dataset updates scheduled every {DATASET_UPDATE_INTERVAL} minutes")
    logger.info(f"KNIME workflow scheduled daily at {KNIME_WORKFLOW_TIME}")

    # Run dataset update immediately on startup
    run_dataset_update()

    while True:
        schedule.run_pending()
        
        # Sleep until the next scheduled job
        time_until_next_job = schedule.idle_seconds()
        if time_until_next_job is None:
            time_until_next_job = 60  # Default to 60 seconds if no jobs are scheduled
        else:
            time_until_next_job = min(time_until_next_job, 3600)  # Cap at 1 hour
        
        logger.info(f"Sleeping for {time_until_next_job / 60:.2f} minutes until next job")
        time.sleep(time_until_next_job)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Update process stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)