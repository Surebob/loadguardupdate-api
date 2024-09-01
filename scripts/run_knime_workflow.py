import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.knime_runner import KNIMERunner
from src.error_handler import KNIMEError
from config.settings import KNIME_EXECUTABLE
from config.logging_config import configure_logging
import logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def main(params=None):
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)
    try:
        logger.info("Attempting to run KNIME workflow")
        output = knime_runner.run_workflow(params)
        logger.info("KNIME workflow executed successfully")
        logger.info(f"Output: {output}")
    except KNIMEError as e:
        logger.error(f"Error running KNIME workflow: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    params = dict(arg.split('=') for arg in sys.argv[1:] if '=' in arg)
    main(params)