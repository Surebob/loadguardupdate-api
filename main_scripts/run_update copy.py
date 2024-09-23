import sys
import os
import asyncio
from datetime import datetime, timedelta
import pytz
import platform  # Import platform to check the operating system
import signal
import logging

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
    MAX_KNIME_RETRIES
)
from src.error_handler import KNIMEError
from config.logging_config import configure_logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_ERROR

configure_logging()
logger = logging.getLogger(__name__)

# Global scheduler variable
scheduler = None

# KNIME retry counter
knime_retry_count = 0

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
    global knime_retry_count
    logger.info("Starting KNIME workflow")
    knime_runner = KNIMERunner()
    try:
        await knime_runner.run_workflow()
        logger.info("KNIME workflow executed successfully")
        knime_retry_count = 0  # Reset the counter on success
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")
        if knime_retry_count < MAX_KNIME_RETRIES:
            knime_retry_count += 1
            logger.warning(f"KNIME workflow failed. Will retry in 5 minutes. Retry {knime_retry_count}/{MAX_KNIME_RETRIES}.")
            await schedule_knime_retry()
        else:
            logger.critical("Maximum KNIME workflow retries reached. No further retries will be scheduled.")

async def schedule_knime_retry():
    retry_time = datetime.now(TIMEZONE) + timedelta(minutes=5)
    logger.info(f"Scheduling KNIME workflow retry at {retry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    # Use the global scheduler
    scheduler.add_job(
        run_knime_workflow,
        DateTrigger(run_date=retry_time),
        id='knime_retry',
        replace_existing=True,
    )

def job_error_listener(event):
    if event.exception:
        logger.error(f"Job {event.job_id} raised an exception: {event.exception}", exc_info=True)
    else:
        logger.info(f"Job {event.job_id} executed successfully")

def handle_shutdown():
    logger.info("Received shutdown signal")
    if scheduler:
        scheduler.shutdown()

async def main():
    global scheduler  # Declare that we are using the global variable
    logger.info("Starting the update process")

    # Run dataset update immediately on startup
    await update_datasets()

    # Initialize the scheduler with timezone awareness
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    # Add the job error listener
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

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

    # Setup signal handlers only on Unix-based systems
    if platform.system() != "Windows":
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown)

    # Keep the script running
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
