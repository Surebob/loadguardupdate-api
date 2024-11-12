from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse
import logging
from datetime import datetime
import aiohttp
import os
import asyncio

from src.services.status_tracker import StatusTracker
from src.socrata_updater import SocrataUpdater
from src.sms_handler import SMSHandler
from src.ftp_handler import FTPHandler
from config.settings import DATA_DIR

router = APIRouter()
logger = logging.getLogger(__name__)
status_tracker = StatusTracker()

@router.get("/status")
async def get_update_status():
    """Get the status of the most recent updates including current progress"""
    updates = status_tracker.get_recent_updates(1)
    
    formatted_updates = []
    
    # Add current downloads if any
    if updates and updates[0]["type"] == "in_progress":
        for dataset, progress in updates[0]["details"].items():
            formatted_updates.append({
                "type": "download",
                "dataset": dataset,
                "progress": progress["progress"],
                "speed": progress["speed"],
                "timestamp": progress["timestamp"]
            })
    
    # Add most recent final status if no downloads are in progress
    if not formatted_updates and updates:
        formatted_updates.append(updates[0])
    
    return formatted_updates

@router.post("/trigger")
async def trigger_updates():
    """Manually trigger the update process"""
    try:
        async with aiohttp.ClientSession() as session:
            # Initialize updaters with status tracker
            socrata_updater = SocrataUpdater(session, status_tracker)
            sms_handler = SMSHandler(session)
            ftp_handler = FTPHandler()

            results = {
                "socrata": False,
                "sms": False,
                "ftp": False
            }

            # Socrata updates
            try:
                status_tracker.log_update("socrata", "updating", {"message": "Starting Socrata updates"})
                results["socrata"] = await socrata_updater.update_and_download_datasets()
                status = "success" if results["socrata"] else "no_update"
                status_tracker.log_update("socrata", status, {"updated": results["socrata"]})
            except Exception as e:
                logger.error(f"Socrata update failed: {str(e)}")
                status_tracker.log_update("socrata", "failed", {"error": str(e)})

            # Similar blocks for SMS and FTP...
            
            return {
                "message": "Update process completed",
                "results": results
            }
    except Exception as e:
        logger.error(f"Update process failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/check")
async def check_updates():
    """Check all data sources for available updates without downloading"""
    try:
        updates_available = {
            "socrata": {},
            "sms": None,
            "ftp": {}
        }

        async with aiohttp.ClientSession() as session:
            # Check Socrata datasets
            socrata_updater = SocrataUpdater(session)
            for dataset_name, dataset_url in socrata_updater.datasets.items():
                try:
                    # Get local date
                    metadata_file = os.path.join(DATA_DIR, dataset_name, f"{dataset_name}_metadata.json")
                    local_date = None
                    if os.path.exists(metadata_file):
                        metadata = await socrata_updater.read_metadata(metadata_file)
                        if metadata and 'rowsUpdatedAt' in metadata:
                            local_date = datetime.fromisoformat(metadata['rowsUpdatedAt'])

                    # Get server date
                    server_date = await socrata_updater.check_dataset_update(dataset_url)

                    updates_available["socrata"][dataset_name] = {
                        "local_date": local_date.isoformat() if local_date else None,
                        "server_date": server_date.isoformat() if server_date else None,
                        "update_needed": server_date > local_date if local_date else True
                    }
                except Exception as e:
                    logger.error(f"Error checking {dataset_name}: {str(e)}")
                    updates_available["socrata"][dataset_name] = {
                        "error": str(e)
                    }

            # Check SMS files
            sms_handler = SMSHandler(session)
            try:
                latest_file = await sms_handler.find_latest_available_file()
                local_files = [f for f in os.listdir(os.path.join(DATA_DIR, 'SMS')) 
                             if f.endswith('.zip')] if os.path.exists(os.path.join(DATA_DIR, 'SMS')) else []
                
                updates_available["sms"] = {
                    "local_file": max(local_files) if local_files else None,
                    "server_file": latest_file,
                    "update_needed": not local_files or latest_file != max(local_files) if local_files else True
                }
            except Exception as e:
                logger.error(f"Error checking SMS files: {str(e)}")
                updates_available["sms"] = {"error": str(e)}

            # Check FTP files
            ftp_handler = FTPHandler()
            for file_type in ['Crash', 'Inspection', 'Violation']:
                try:
                    latest_remote = await ftp_handler.find_latest_file(file_type)
                    local_dir = os.path.join(DATA_DIR, f'FTP_{file_type}')
                    latest_local = ftp_handler.find_latest_local_file(local_dir, file_type)

                    updates_available["ftp"][file_type] = {
                        "local_file": latest_local,
                        "server_file": latest_remote,
                        "update_needed": latest_remote != latest_local if latest_local else True
                    }
                except Exception as e:
                    logger.error(f"Error checking FTP {file_type}: {str(e)}")
                    updates_available["ftp"][file_type] = {"error": str(e)}

        return updates_available

    except Exception as e:
        logger.error(f"Update check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 

@router.get("/stream")
async def stream_updates(request: Request):
    """Stream real-time updates using Server-Sent Events"""
    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            # Get current progress from status tracker
            current_progress = status_tracker.current_progress
            
            if current_progress:
                # We have active downloads
                yield {
                    "event": "update",
                    "data": {
                        "type": "download",
                        "datasets": current_progress
                    }
                }
            else:
                # Get latest status
                updates = status_tracker.get_recent_updates(1)
                if updates:
                    yield {
                        "event": "update",
                        "data": {
                            "type": "status",
                            "updates": updates
                        }
                    }

            await asyncio.sleep(1)  # Check every second

    return EventSourceResponse(event_generator())