import sys
import os
import asyncio
from datetime import datetime, timedelta
import pytz
import platform
import signal
import logging
import time
import aiohttp
import pyautogui
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.socrata_updater import SocrataUpdater
from src.ftp_handler import FTPHandler
from src.sms_handler import SMSHandler
from main_scripts.knimeclicker import perform_clicks
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
from src.zip_processor import ZipProcessor

# Set the flag file path relative to the script's directory
FLAG_FILE = os.path.join(os.path.dirname(__file__), '..', 'script_running.flag')

# Global variables
keep_updating_flag = True
scheduler = None
knime_retry_count = 0

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Define the coordinates for the clicks
first_click_coordinates = (207, 90)  # Replace with your coordinates
second_click_coordinates = (408, 89)  # Replace with your coordinates

async def update_flag_file():
    while True:
        try:
            with open(FLAG_FILE, 'w') as f:
                f.write(str(time.time()))
            logger.debug(f"Flag file updated at {time.time()}")
        except Exception as e:
            logger.error(f"Failed to update flag file: {e}")
        await asyncio.sleep(0.2)  # Update every 0.2 seconds instead of 1 second

async def update_datasets():
    logger.info("Starting dataset update")
    updated = False

    try:
        # Create session with custom timeout
        timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=3600)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Process Socrata datasets
            socrata_updater = SocrataUpdater(session)
            if await socrata_updater.update_and_download_datasets():
                updated = True

            # Process SMS file
            sms_handler = SMSHandler(session)
            if await sms_handler.download_latest_sms_file():
                updated = True

        # Process FTP files (separate from HTTP session)
        ftp_handler = FTPHandler()
        if await ftp_handler.download_ftp_files():
            updated = True

        if updated:
            logger.info("Datasets have been updated")
        else:
            logger.info("No updates found for datasets")

        # Process ZIP files after all updates are complete
        zip_processor = ZipProcessor(DATA_DIR)
        if await zip_processor.process_all_zips():
            logger.info("ZIP files processed successfully")
        else:
            logger.info("No ZIP files needed processing")

        # Add next scheduled runs info
        if scheduler:
            next_dataset_run = scheduler.get_job('dataset_update').next_run_time
            next_click_run = scheduler.get_job('perform_clicks_job').next_run_time
            logger.info(f"Next dataset update scheduled for: {next_dataset_run}")
            logger.info(f"Next click sequence scheduled for: {next_click_run}")
            logger.info("Waiting for next scheduled run...")

    except Exception as e:
        logger.error(f"Error during dataset update: {str(e)}")
        raise

def job_error_listener(event):
    if event.exception:
        logger.error(f"Job {event.job_id} raised an exception: {event.exception}", exc_info=True)
        logger.error("Exception details:", exc_info=event.exception)
    else:
        logger.info(f"Job {event.job_id} executed successfully")

def handle_shutdown():
    global keep_updating_flag
    logger.info("Received shutdown signal")
    keep_updating_flag = False
    if scheduler:
        scheduler.shutdown()

async def main():
    global scheduler, keep_updating_flag
    logger.info("Starting the update process")

    try:
        # Start the flag file update task FIRST
        flag_update_task = asyncio.create_task(update_flag_file())
        
        # Initialize the scheduler with timezone awareness
        scheduler = AsyncIOScheduler(timezone=TIMEZONE)
        scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

        # Schedule dataset updates
        dataset_update_hour, dataset_update_minute = map(int, DATASET_UPDATE_TIME.split(":"))
        scheduler.add_job(
            update_datasets,
            CronTrigger(
                hour=dataset_update_hour, 
                minute=dataset_update_minute, 
                timezone=TIMEZONE
            ),
            id='dataset_update',
            name='Dataset Update Job'
        )
        logger.info(f"Scheduled dataset updates daily at {DATASET_UPDATE_TIME} {TIMEZONE}")

        # Schedule the perform_clicks function
        click_time_hour, click_time_minute = map(int, KNIME_WORKFLOW_TIME.split(":"))
        scheduler.add_job(
            perform_clicks,
            CronTrigger(
                hour=click_time_hour,
                minute=click_time_minute,
                timezone=TIMEZONE
            ),
            id='perform_clicks_job',
            name='Click Sequence Job'
        )
        logger.info(f"Scheduled click sequence daily at {KNIME_WORKFLOW_TIME} {TIMEZONE}")

        # Start the scheduler before running initial update
        scheduler.start()
        logger.info("Scheduler started and running")
        
        # Log all scheduled jobs
        jobs = scheduler.get_jobs()
        logger.info("Currently scheduled jobs:")
        for job in jobs:
            next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S %Z") if job.next_run_time else "Not scheduled"
            logger.info(f"Job ID: {job.id}, Name: {job.name}, Next run: {next_run}")

        # Run initial update
        logger.info("Running initial update...")
        await update_datasets()
        logger.info("Initial update completed")

        # Show next scheduled runs after initial completion
        next_dataset_run = scheduler.get_job('dataset_update').next_run_time
        next_click_run = scheduler.get_job('perform_clicks_job').next_run_time
        logger.info(f"Next scheduled dataset update: {next_dataset_run}")
        logger.info(f"Next scheduled clicks: {next_click_run}")
        logger.info("Waiting for next scheduled run...")

        # Keep the script running and monitor scheduler
        while keep_updating_flag:
            await asyncio.sleep(60)  # Check every minute instead of every second
            if not scheduler or not scheduler.running:
                logger.error("Scheduler stopped running unexpectedly")
                scheduler.start()
                logger.info("Scheduler restarted")

    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        handle_shutdown()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        handle_shutdown()
    finally:
        # Clean up
        if 'flag_update_task' in locals():
            flag_update_task.cancel()
            try:
                await flag_update_task
            except asyncio.CancelledError:
                pass
        
        if scheduler and scheduler.running:
            scheduler.shutdown()
        
        if os.path.exists(FLAG_FILE):
            try:
                os.remove(FLAG_FILE)
            except Exception as e:
                logger.error(f"Error removing flag file: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
