from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from config.logging_config import configure_logging
from config.settings import TIMEZONE
from datetime import datetime
from api.routes import scheduler as scheduler_router
from api.routes import updates, status
from src.services.scheduler_instance import scheduler
from main_scripts.knimeclicker import perform_clicks
from api.routes.updates import trigger_updates
from src.services.config_manager import ConfigManager

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Initialize ConfigManager
config_manager = ConfigManager()

# Initialize FastAPI
app = FastAPI(
    title="LoadGuard Update API",
    description="API for managing and monitoring LoadGuard data updates",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(scheduler_router.router, prefix="/api/scheduler", tags=["scheduler"])
app.include_router(updates.router, prefix="/api/updates", tags=["updates"])
app.include_router(status.router, prefix="/api/status", tags=["status"])

@app.on_event("startup")
async def startup_event():
    # Start the scheduler
    scheduler.start()
    
    # Get schedule from config file
    schedule = config_manager.get_schedule()
    dataset_hour, dataset_minute = map(int, schedule["dataset_update_time"].split(':'))
    clicker_hour, clicker_minute = map(int, schedule["clicker_schedule_time"].split(':'))
    
    # Schedule dataset updates
    scheduler.add_job(
        trigger_updates,
        'cron',
        hour=dataset_hour,
        minute=dataset_minute,
        timezone=TIMEZONE,
        id='dataset_update',
        name='Dataset Updates'
    )
    
    # Schedule clicker
    scheduler.add_job(
        perform_clicks,
        'cron',
        hour=clicker_hour,
        minute=clicker_minute,
        timezone=TIMEZONE,
        id='clicker_job',
        name='Mouse Clicker'
    )
    
    logger.info(f"API Server started, scheduler initialized with jobs from config: {schedule}")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
    logger.info("API Server shutting down, scheduler stopped")

@app.get("/")
async def root():
    return {
        "status": "running",
        "current_time": datetime.now(TIMEZONE).isoformat(),
        "scheduler_running": scheduler.running
    } 