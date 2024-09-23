# knime_runner.py
import sys
import os
import platform
import subprocess
import asyncio

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.error_handler import KNIMEError
from config.settings import KNIME_WORKFLOW_DIR, KNIME_EXECUTABLE, BASE_DIR
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class KNIMERunner:
    def __init__(self):
        self.knime_executable = KNIME_EXECUTABLE
        self.log_dir = os.path.join(BASE_DIR, 'logs')
        os.makedirs(self.log_dir, exist_ok=True)

    async def run_workflow(self):
        try:
            command = [
                self.knime_executable,
                "-reset",
                "-nosplash",
                "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
                f"-workflowDir={KNIME_WORKFLOW_DIR}",
                "--launcher.suppressErrors"
            ]

            # Create a unique log file name with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(self.log_dir, f"knime_output_{timestamp}.log")

            # Open the log file
            with open(log_file, 'w') as f:
                start_time = datetime.now()
                f.write(f"KNIME workflow started at: {start_time}\n")
                f.flush()

                # Set creationflags conditionally based on the OS
                creationflags = 0
                if platform.system() == "Windows":
                    creationflags = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP

                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    creationflags=creationflags if platform.system() == "Windows" else None
                )

                async def log_stream(stream):
                    while True:
                        line = await stream.readline()
                        if line:
                            log_message = line.decode().strip()
                            f.write(f"{log_message}\n")
                            f.flush()  # Ensure the message is written immediately
                        else:
                            break

                # Only log to the file, not to the console
                await asyncio.gather(
                    log_stream(process.stdout),
                    log_stream(process.stderr)
                )

                return_code = await process.wait()

                end_time = datetime.now()
                duration = end_time - start_time
                f.write(f"\nKNIME workflow ended at: {end_time}\n")
                f.write(f"Total duration: {duration}\n")
                f.flush()

            if return_code != 0:
                error_message = f"KNIME workflow execution failed with exit code {return_code}. Check log file for details: {log_file}"
                logger.error(error_message)
                raise KNIMEError(error_message)

            logger.info(f"KNIME workflow completed successfully. Output logged to {log_file}")

        except subprocess.CalledProcessError as e:
            error_message = f"KNIME workflow execution failed. Exit code: {e.returncode}. Check log file for details."
            logger.error(error_message)
            raise KNIMEError(error_message)
        except Exception as e:
            logger.error(f"Unexpected error while running KNIME workflow: {str(e)}")
            raise KNIMEError(f"Unexpected error while running KNIME workflow: {str(e)}")

async def main():
    runner = KNIMERunner()
    try:
        await runner.run_workflow()
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
