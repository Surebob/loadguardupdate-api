# run_update.py

import sys
import os
import asyncio
from datetime import datetime, timedelta
import pytz

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.socrata_api import SocrataAPI
from main_scripts.knime_runner import KNIMERunner
from src.zip_file_handler import ZipFileHandler
from src.dropbox_handler import DropboxHandler
from config.settings import (
    KNIME_EXECUTABLE,
    DATA_DIR,
    TIMEZONE,
    DATASET_UPDATE_TIME,
    KNIME_WORKFLOW_TIME,
)
from src.error_handler import KNIMEError, APIError
from config.logging_config import configure_logging
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

configure_logging()
logger = logging.getLogger(__name__)

async def update_datasets():
    logger.info("Starting dataset update")
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
    logger.info("Starting KNIME workflow")
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)
    try:
        await knime_runner.run_workflow()
        logger.info("KNIME workflow executed successfully")
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")
        logger.warning("KNIME workflow failed. Will retry in 5 minutes.")
        await schedule_knime_retry()

async def schedule_knime_retry():
    retry_time = datetime.now(TIMEZONE) + timedelta(minutes=5)
    logger.info(f"Scheduling KNIME workflow retry at {retry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        run_knime_workflow,
        DateTrigger(run_date=retry_time),
        id='knime_retry',
        replace_existing=True,
    )
    scheduler.start()

async def main():
    logger.info("Starting the update process")

    # Run dataset update immediately on startup
    await update_datasets()

    # Initialize the scheduler with timezone awareness
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    # Schedule the dataset updates at the specified time
    dataset_update_hour, dataset_update_minute = map(int, DATASET_UPDATE_TIME.split(":"))
    scheduler.add_job(
        update_datasets,
        CronTrigger(
            hour=dataset_update_hour, minute=dataset_update_minute, timezone=TIMEZONE
        ),
        id='dataset_update',
    )
    logger.info(f"Scheduled dataset updates daily at {DATASET_UPDATE_TIME} {TIMEZONE}")

    # Schedule the KNIME workflow at the specified time
    knime_workflow_hour, knime_workflow_minute = map(int, KNIME_WORKFLOW_TIME.split(":"))
    scheduler.add_job(
        run_knime_workflow,
        CronTrigger(
            hour=knime_workflow_hour, minute=knime_workflow_minute, timezone=TIMEZONE
        ),
        id='knime_workflow',
    )
    logger.info(f"Scheduled KNIME workflow daily at {KNIME_WORKFLOW_TIME} {TIMEZONE}")

    # Start the scheduler
    scheduler.start()

    # Keep the script running
    try:
        await asyncio.Event().wait()  # This creates an event that never gets set, so it waits indefinitely
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
