from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config.settings import TIMEZONE

# Create a single scheduler instance
scheduler = AsyncIOScheduler(timezone=TIMEZONE) 