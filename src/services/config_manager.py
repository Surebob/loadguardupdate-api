import json
import os
import logging
from typing import Dict, Optional
from config.settings import BASE_DIR

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self):
        self.config_dir = os.path.join(BASE_DIR, "config")
        self.config_file = os.path.join(self.config_dir, "schedule_config.json")
        self.config = self.load_config()

    def load_config(self) -> Dict:
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
        
        # Return default config if file doesn't exist or has error
        from config.settings import DATASET_UPDATE_TIME, CLICKER_SCHEDULE_TIME
        return {
            "dataset_update_time": DATASET_UPDATE_TIME,
            "clicker_schedule_time": CLICKER_SCHEDULE_TIME
        }

    def save_config(self) -> None:
        """Save configuration to file"""
        try:
            os.makedirs(self.config_dir, exist_ok=True)
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Schedule configuration saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Error saving config: {e}")

    def update_schedule(self, dataset_time: Optional[str] = None, clicker_time: Optional[str] = None) -> None:
        """Update schedule times"""
        if dataset_time:
            self.config["dataset_update_time"] = dataset_time
            logger.info(f"Updated dataset update time to {dataset_time}")
        if clicker_time:
            self.config["clicker_schedule_time"] = clicker_time
            logger.info(f"Updated clicker schedule time to {clicker_time}")
        self.save_config()

    def get_schedule(self) -> Dict[str, str]:
        """Get current schedule times"""
        return {
            "dataset_update_time": self.config["dataset_update_time"],
            "clicker_schedule_time": self.config["clicker_schedule_time"]
        } 