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

def format_time_until_knime(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours:.0f} hours and {minutes:.0f} minutes"
    else:
        return f"{minutes:.0f} minutes"

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

    last_knime_log_time = datetime.now(TIMEZONE) - timedelta(minutes=30)

    while True:
        now = datetime.now(TIMEZONE)
        
        # Check if it's time to run scheduled jobs
        schedule.run_pending()
        
        # Calculate time until next KNIME run
        knime_time = datetime.strptime(KNIME_WORKFLOW_TIME, "%H:%M").time()
        next_knime_run = TIMEZONE.localize(datetime.combine(now.date(), knime_time))
        if next_knime_run <= now:
            next_knime_run += timedelta(days=1)
        time_until_knime = (next_knime_run - now).total_seconds()

        # Log KNIME update time every 30 minutes
        if (now - last_knime_log_time).total_seconds() >= 1800:  # 30 minutes
            knime_time_str = format_time_until_knime(time_until_knime)
            logger.info(f"KNIME Update Workflow will run in {knime_time_str}")
            last_knime_log_time = now

        # Calculate time until next dataset update
        next_dataset_update = schedule.next_run()
        time_until_dataset_update = (next_dataset_update - now).total_seconds()

        # Sleep until the next event (dataset update or KNIME run)
        sleep_time = min(time_until_dataset_update, time_until_knime, 60)  # Max sleep of 60 seconds
        logger.info(f"Sleeping for {sleep_time:.2f} seconds until next check")
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Update process stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)