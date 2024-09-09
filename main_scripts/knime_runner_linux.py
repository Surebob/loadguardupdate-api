import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import subprocess
import asyncio
from src.error_handler import KNIMEError
from config.settings import KNIME_WORKFLOW_DIR, BASE_DIR
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KNIMERunner:
    def __init__(self):
        self.log_dir = os.path.join(BASE_DIR, 'logs')
        os.makedirs(self.log_dir, exist_ok=True)

    async def run_workflow(self):
        try:
            command = [
                "knime",
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

                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
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
                raise subprocess.CalledProcessError(return_code, command)

            logger.info(f"KNIME workflow completed successfully. Output logged to {log_file}")

        except subprocess.CalledProcessError as e:
            error_message = f"KNIME workflow execution failed. Exit code: {e.returncode}. Check log file for details."
            raise KNIMEError(error_message)

async def main():
    runner = KNIMERunner()
    try:
        await runner.run_workflow()
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())