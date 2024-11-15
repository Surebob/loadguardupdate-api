import sys
import time

class ProgressBar:
    def __init__(self, description="Downloading", **kwargs):
        self.description = description
        self.start_time = None
        self.last_update = 0
        self.update_interval = 0.5
        # Store status tracker and dataset name if provided
        self.status_tracker = kwargs.get('status_tracker')
        self.dataset_name = kwargs.get('dataset_name')

    def start(self):
        self.start_time = time.time()
        self.last_update = self.start_time
        sys.stdout.write(f"\r{self.description}: 0.0MB [0.0MB/s]")
        sys.stdout.flush()

    def update(self, downloaded_size):
        current_time = time.time()
        if current_time - self.last_update >= self.update_interval:
            mb_downloaded = downloaded_size / (1024 * 1024)
            elapsed_time = max(current_time - self.start_time, 0.1)
            speed = mb_downloaded / elapsed_time
            
            # Update status tracker if available
            if self.status_tracker and self.dataset_name:
                self.status_tracker.update_progress(self.dataset_name, mb_downloaded, speed)
            
            sys.stdout.write(f"\r{self.description}: {mb_downloaded:.1f}MB [{speed:.1f}MB/s]")
            sys.stdout.flush()
            self.last_update = current_time

    def finish(self):
        if self.status_tracker and self.dataset_name:
            self.status_tracker.clear_progress(self.dataset_name)
        sys.stdout.write("\n")
        sys.stdout.flush()
