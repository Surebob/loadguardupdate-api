import os

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directory to store downloaded data
DATA_DIR = os.path.join(BASE_DIR, "data")

# KNIME settings
KNIME_EXECUTABLE = "/path/to/knime"
KNIME_WORKFLOW_DIR = "/path/to/existing/knime/workflow/directory"

# Update interval in hours
CHECK_INTERVAL_HOURS = 1

# Logging configuration
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'application.log')
LOG_LEVEL = 'INFO'

# Dataset configurations are now handled in the SocrataAPI class
# If you need to reference dataset names elsewhere in your application,
# you can define them here:
DATASET_NAMES = [
    'ActPendInsur',
    'AuthHist',
    'CarrierAllWithHistory',
    'NewCompanyCensusFile',
    'VehicleInspectionsFile',
    'InspectionPerUnit',
    'InsurAllWithHistory',
    'CrashFile'
]

# Dropbox dataset names
DROPBOX_DATASET_NAMES = [
    'CENSUS_PUB_20240509_1of3',
    'CENSUS_PUB_20240509_2of3',
    'CENSUS_PUB_20240509_3of3'
]