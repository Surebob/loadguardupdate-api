import pyautogui
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Define the coordinates for the clicks
FIRST_CLICK_COORDINATES = (207, 90)  # Replace with your coordinates
SECOND_CLICK_COORDINATES = (408, 89)  # Replace with your coordinates

async def perform_clicks():
    """Performs the automated clicking sequence."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting perform_clicks function at {current_time}")
    try:
        # Log mouse position before clicking
        current_pos = pyautogui.position()
        logger.info(f"Current mouse position before clicks: {current_pos}")
        
        logger.info("Starting first click sequence...")
        # Click the first point multiple times
        for i in range(10):
            pyautogui.click(*SECOND_CLICK_COORDINATES)
            logger.info(f"Completed click {i+1}/10 at position {SECOND_CLICK_COORDINATES}")
            await asyncio.sleep(1)

        logger.info("First click sequence completed. Starting 3-hour wait...")
        # Wait for 3 hours
        logger.info("Starting second click sequence...")
        # Click the second point multiple times
        for i in range(10):
            pyautogui.click(*FIRST_CLICK_COORDINATES)
            logger.info(f"Completed click {i+1}/10 at position {FIRST_CLICK_COORDINATES}")
            await asyncio.sleep(1)

        logger.info("perform_clicks function completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error in perform_clicks: {str(e)}", exc_info=True)
        return False
