import sys
import time

class ProgressBar:
    def __init__(self, description="Downloading"):
        self.description = description
        self.start_time = None
        self.last_update = 0
        self.update_interval = 0.5  # Update every 0.5 seconds

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
            sys.stdout.write(f"\r{self.description}: {mb_downloaded:.1f}MB [{speed:.1f}MB/s]")
            sys.stdout.flush()
            self.last_update = current_time

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()
