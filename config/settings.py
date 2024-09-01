import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SOCRATA_BASE_URL = "https://datahub.transportation.gov"
SOCRATA_APP_TOKEN = "YOUR_API_KEY_HERE"
KNIME_EXECUTABLE = "/path/to/knime"
KNIME_WORKFLOW_DIR = "/path/to/existing/knime/workflow/directory"
DATA_DIR = os.path.join(BASE_DIR, "data")

DATASETS = [
    {"id": "qh9u-swkp", "name": "Primary Dataset", "category": "INSPECTIONS"},
    {"id": "placeholder1", "name": "Dataset 1", "category": "CENSUS"},
    # Add more datasets as needed
]

CHECK_INTERVAL_HOURS = 1