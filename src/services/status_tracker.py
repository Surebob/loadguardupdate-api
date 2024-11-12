import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class StatusTracker:
    def __init__(self):
        self.status_file = "update_history.json"
        self.history: List[Dict] = []
        self.current_progress: Dict = {}  # Store current download progress
        self.load_history()

    def load_history(self):
        """Load update history from file if it exists"""
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r') as f:
                    self.history = json.load(f)
        except Exception as e:
            logger.error(f"Error loading history: {e}")
            self.history = []

    def save_history(self):
        """Save update history to file"""
        try:
            with open(self.status_file, 'w') as f:
                json.dump(self.history, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving history: {e}")

    def update_progress(self, dataset_name: str, downloaded: float, speed: float):
        """Update current download progress"""
        self.current_progress[dataset_name] = {
            "status": "downloading",
            "progress": f"{downloaded:.1f}MB",
            "speed": f"{speed:.1f}MB/s",
            "timestamp": datetime.utcnow().isoformat()
        }

    def clear_progress(self, dataset_name: str):
        """Clear progress for a dataset"""
        if dataset_name in self.current_progress:
            del self.current_progress[dataset_name]

    def log_update(self, update_type: str, status: str, details: Dict):
        """Log an update event"""
        update_log = {
            "type": update_type,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details
        }
        self.history.append(update_log)
        self.save_history()
        return update_log

    def get_recent_updates(self, limit: int = 10) -> List[Dict]:
        """Get the most recent updates and current progress"""
        updates = sorted(
            self.history,
            key=lambda x: x["timestamp"],
            reverse=True
        )[:limit]
        
        # Include current progress if any
        if self.current_progress:
            updates.insert(0, {
                "type": "in_progress",
                "status": "downloading",
                "timestamp": datetime.utcnow().isoformat(),
                "details": self.current_progress
            })
        
        return updates

    def get_latest_status(self, update_type: str) -> Optional[Dict]:
        """Get the latest status for a specific update type"""
        updates = [u for u in self.history if u["type"] == update_type]
        return max(updates, key=lambda x: x["timestamp"]) if updates else None 