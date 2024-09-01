from src.knime_runner import KNIMERunner
from src.error_handler import KNIMEError
from config.settings import KNIME_EXECUTABLE
import sys
import logging

def main(params=None):
    knime_runner = KNIMERunner(KNIME_EXECUTABLE)
    try:
        output = knime_runner.run_workflow(params)
        logging.info(f"KNIME workflow executed successfully")
        logging.info(f"Output: {output}")
    except KNIMEError as e:
        logging.error(f"Error running KNIME workflow: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    params = dict(arg.split('=') for arg in sys.argv[1:] if '=' in arg)
    main(params)