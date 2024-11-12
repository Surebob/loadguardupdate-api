from fastapi import APIRouter, Depends
import psutil
import os
from datetime import datetime
from typing import Dict, Any

from config.settings import DATA_DIR, TIMEZONE
from src.services.status_tracker import StatusTracker

router = APIRouter()
status_tracker = StatusTracker()

@router.get("/system")
async def get_system_status():
    """Get overall system status including CPU, memory, and heartbeat"""
    return {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": psutil.virtual_memory().percent,
        "heartbeat": True,
        "timestamp": datetime.now(TIMEZONE).isoformat()
    }

@router.get("/datasets")
async def get_dataset_status():
    """Get status of all datasets including last update times"""
    dataset_info = {}
    
    # Get status for each dataset type
    for update_type in ["socrata", "sms", "ftp"]:
        latest_update = status_tracker.get_latest_status(update_type)
        dataset_info[update_type] = {
            "last_update": latest_update["timestamp"] if latest_update else None,
            "status": latest_update["status"] if latest_update else "unknown",
            "details": latest_update["details"] if latest_update else None
        }
    
    return dataset_info

@router.get("/history")
async def get_update_history(limit: int = 50, update_type: str = None):
    """Get update history with optional filtering"""
    if update_type:
        latest_status = status_tracker.get_latest_status(update_type)
        return [latest_status] if latest_status else []
    else:
        return status_tracker.get_recent_updates(limit)