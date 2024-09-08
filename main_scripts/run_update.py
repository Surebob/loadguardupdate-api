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
                             DATASET_UPDATE_INTERVAL, KNIME_WORKFLOW_TIME, CHECK_INTERVAL)
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
        return True
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")
        return False

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

def format_sleep_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = int(seconds % 60)
        if remaining_seconds == 0:
            return f"{minutes} minutes"
        else:
            return f"{minutes} minutes and {remaining_seconds} seconds"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        if remaining_minutes == 0:
            return f"{hours} hours"
        else:
            return f"{hours} hours and {remaining_minutes} minutes"

def main():
    logger.info("Starting the update process")

    # Run dataset update immediately on startup
    run_dataset_update()

    last_update_time = datetime.now(TIMEZONE)
    last_check_time = last_update_time
    last_knime_log_time = last_update_time - timedelta(minutes=30)
    last_knime_run_date = last_update_time.date() - timedelta(days=1)  # Ensure it runs on first day
    knime_retry_time = None

    while True:
        now = datetime.now(TIMEZONE)
        
        # Check if it's time for dataset update (every 3 hours)
        if (now - last_update_time).total_seconds() >= DATASET_UPDATE_INTERVAL * 3600:
            run_dataset_update()
            last_update_time = now
            logger.info("Dataset update completed")

        # Check if it's time for KNIME workflow
        knime_time = datetime.strptime(KNIME_WORKFLOW_TIME, "%H:%M").time()
        knime_datetime = TIMEZONE.localize(datetime.combine(now.date(), knime_time))
        
        if (now.date() > last_knime_run_date and now >= knime_datetime) or (knime_retry_time and now >= knime_retry_time):
            knime_success = asyncio.run(run_knime_workflow())
            if knime_success:
                logger.info("KNIME workflow executed successfully")
                last_knime_run_date = now.date()
                knime_retry_time = None
            else:
                logger.warning("KNIME workflow failed. Will retry in 5 minutes.")
                knime_retry_time = now + timedelta(minutes=5)

        # Calculate time until next KNIME run
        next_knime_run = knime_datetime
        if next_knime_run <= now:
            next_knime_run += timedelta(days=1)
        time_until_knime = (next_knime_run - now).total_seconds()

        # Log KNIME update time every 30 minutes
        if (now - last_knime_log_time).total_seconds() >= 1800:  # 30 minutes
            knime_time_str = format_sleep_time(time_until_knime)
            logger.info(f"KNIME Update Workflow will run in {knime_time_str}")
            last_knime_log_time = now

        # Calculate time until next check
        time_until_next_check = CHECK_INTERVAL * 60 - (now - last_check_time).total_seconds()

        # Calculate time until KNIME retry if applicable
        time_until_knime_retry = (knime_retry_time - now).total_seconds() if knime_retry_time else float('inf')

        # Sleep until the next event (check, KNIME run, or KNIME retry)
        sleep_time = max(0, min(time_until_next_check, time_until_knime, time_until_knime_retry))
        formatted_sleep_time = format_sleep_time(sleep_time)
        logger.info(f"Sleeping for {formatted_sleep_time} until next check")
        time.sleep(sleep_time)

        last_check_time = now

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Update process stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)