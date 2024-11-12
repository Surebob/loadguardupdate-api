from fastapi import APIRouter, HTTPException
import logging
from typing import Dict, Optional
from datetime import datetime
from pydantic import BaseModel

from src.services.scheduler_instance import scheduler
from config.settings import TIMEZONE
from main_scripts.knimeclicker import perform_clicks
from api.routes.updates import trigger_updates
from src.services.status_tracker import StatusTracker
from src.services.config_manager import ConfigManager

router = APIRouter()
logger = logging.getLogger(__name__)
status_tracker = StatusTracker()
config_manager = ConfigManager()

class ScheduleUpdate(BaseModel):
    dataset_time: Optional[str]
    clicker_time: Optional[str]

@router.get("/status")
async def get_scheduler_status():
    """Get current scheduler status and next run times"""
    jobs = scheduler.get_jobs()
    job_info = {}
    
    for job in jobs:
        job_info[job.id] = {
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "running": job.pending
        }
    
    return {
        "scheduler_running": scheduler.running,
        "jobs": job_info,
        "current_time": datetime.now(TIMEZONE).isoformat()
    }

@router.post("/update-schedule")
async def update_schedule(schedule: ScheduleUpdate):
    """Update the schedule times for updates and clicker"""
    try:
        if schedule.dataset_time:
            try:
                scheduler.remove_job('dataset_update')
            except:
                pass
            hour, minute = map(int, schedule.dataset_time.split(':'))
            scheduler.add_job(
                trigger_updates,
                'cron',
                hour=hour,
                minute=minute,
                timezone=TIMEZONE,
                id='dataset_update',
                name='Dataset Updates'
            )
            
        if schedule.clicker_time:
            try:
                scheduler.remove_job('clicker_job')
            except:
                pass
            hour, minute = map(int, schedule.clicker_time.split(':'))
            scheduler.add_job(
                perform_clicks,
                'cron',
                hour=hour,
                minute=minute,
                timezone=TIMEZONE,
                id='clicker_job',
                name='Mouse Clicker'
            )
            
        config_manager.update_schedule(
            dataset_time=schedule.dataset_time,
            clicker_time=schedule.clicker_time
        )
            
        return {"message": "Schedule updated successfully"}
    except Exception as e:
        logger.error(f"Failed to update schedule: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/pause")
async def pause_scheduler():
    """Pause all scheduled jobs"""
    try:
        scheduler.pause()
        return {"message": "Scheduler paused"}
    except Exception as e:
        logger.error(f"Failed to pause scheduler: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/resume")
async def resume_scheduler():
    """Resume all scheduled jobs"""
    try:
        scheduler.resume()
        return {"message": "Scheduler resumed"}
    except Exception as e:
        logger.error(f"Failed to resume scheduler: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))