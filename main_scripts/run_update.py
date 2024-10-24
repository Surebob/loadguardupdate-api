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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.socrata_updater import SocrataUpdater
from src.ftp_handler import FTPHandler
from src.sms_handler import SMSHandler
from main_scripts.knime_runner import KNIMERunner
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

FLAG_FILE = "script_running.flag"

# Global variables
keep_updating_flag = True
scheduler = None
knime_retry_count = 0

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

async def update_flag_file():
    global keep_updating_flag
    while keep_updating_flag:
        with open(FLAG_FILE, 'w') as f:
            f.write(str(time.time()))
        await asyncio.sleep(1)

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
        if zip_processor.process_all_zips():
            logger.info("ZIP files processed successfully")
        else:
            logger.info("No ZIP files needed processing")

    except Exception as e:
        logger.error(f"Error during dataset update: {str(e)}")
        raise

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
    global keep_updating_flag
    logger.info("Received shutdown signal")
    keep_updating_flag = False
    if scheduler:
        scheduler.shutdown()

async def main():
    global scheduler, keep_updating_flag
    logger.info("Starting the update process")

    # Start the flag file update task
    flag_update_task = asyncio.create_task(update_flag_file())
    
    try:
        # Run dataset update immediately on startup
        await update_datasets()

        # Initialize the scheduler with timezone awareness
        scheduler = AsyncIOScheduler(timezone=TIMEZONE)
        scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

        # Schedule dataset updates
        dataset_update_hour, dataset_update_minute = map(int, DATASET_UPDATE_TIME.split(":"))
        scheduler.add_job(
            update_datasets,
            CronTrigger(
                hour=dataset_update_hour, minute=dataset_update_minute, timezone=TIMEZONE
            ),
            id='dataset_update',
        )
        logger.info(f"Scheduled dataset updates daily at {DATASET_UPDATE_TIME} {TIMEZONE}")

        # Schedule KNIME workflow
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

        # Setup signal handlers
        if platform.system() != "Windows":
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, handle_shutdown)

        # Keep the script running
        while keep_updating_flag:
            await asyncio.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        handle_shutdown()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        handle_shutdown()
    finally:
        # Clean up flag update task
        flag_update_task.cancel()
        try:
            await flag_update_task
        except asyncio.CancelledError:
            pass
        
        # Remove flag file if it exists
        if os.path.exists(FLAG_FILE):
            try:
                os.remove(FLAG_FILE)
            except Exception as e:
                logger.error(f"Error removing flag file: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
