# knime_runner.py
import sys
import os
import platform
import subprocess
import asyncio
import traceback
from pathlib import Path

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
            # Verify KNIME executable exists
            if not os.path.exists(self.knime_executable):
                raise KNIMEError(f"KNIME executable not found at: {self.knime_executable}")

            # Verify workflow directory exists
            if not os.path.exists(KNIME_WORKFLOW_DIR):
                raise KNIMEError(f"KNIME workflow directory not found at: {KNIME_WORKFLOW_DIR}")

            # Create command with proper path handling
            command = [
                str(Path(self.knime_executable)),
                "-reset",
                "-nosplash",
                "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
                f"-workflowDir={str(Path(KNIME_WORKFLOW_DIR))}",
                "--launcher.suppressErrors"
            ]

            # Create a unique log file name with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(self.log_dir, f"knime_output_{timestamp}.log")

            # Ensure log directory exists and is writable
            os.makedirs(os.path.dirname(log_file), exist_ok=True)

            # Log the command being executed
            logger.debug(f"Executing KNIME command: {' '.join(command)}")

            def run_process():
                try:
                    # Use subprocess.run with shell=True for Windows
                    result = subprocess.run(
                        ' '.join(command),
                        shell=True,
                        capture_output=True,
                        text=True,
                        cwd=os.path.dirname(self.knime_executable)
                    )
                    return result
                except Exception as e:
                    logger.error(f"Error in subprocess execution: {str(e)}")
                    raise

            # Run the process in the executor
            loop = asyncio.get_event_loop()
            with open(log_file, 'w') as f:
                start_time = datetime.now()
                f.write(f"KNIME workflow started at: {start_time}\n")
                f.flush()

                try:
                    # Run the process in the executor
                    result = await loop.run_in_executor(None, run_process)

                    # Log output
                    if result.stdout:
                        f.write("\nStandard Output:\n")
                        f.write(result.stdout)
                    if result.stderr:
                        f.write("\nStandard Error:\n")
                        f.write(result.stderr)

                    end_time = datetime.now()
                    duration = end_time - start_time
                    f.write(f"\nKNIME workflow ended at: {end_time}\n")
                    f.write(f"Total duration: {duration}\n")
                    f.write(f"Return code: {result.returncode}\n")
                    f.flush()

                    if result.returncode != 0:
                        error_message = f"KNIME workflow execution failed with exit code {result.returncode}. Check log file for details: {log_file}"
                        logger.error(error_message)
                        raise KNIMEError(error_message)

                    logger.info(f"KNIME workflow completed successfully. Output logged to {log_file}")

                except Exception as e:
                    error_message = f"Error executing KNIME workflow: {str(e)}"
                    logger.error(error_message)
                    logger.error(traceback.format_exc())
                    raise KNIMEError(error_message)

        except Exception as e:
            error_message = f"Unexpected error while running KNIME workflow: {str(e)}"
            logger.error(error_message)
            raise KNIMEError(error_message)

async def main():
    runner = KNIMERunner()
    try:
        await runner.run_workflow()
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
