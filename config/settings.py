# settings.py

import os
import pytz

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directory to store downloaded data
DATA_DIR = os.path.join(BASE_DIR, "data")

# KNIME settings
KNIME_EXECUTABLE = os.environ.get('KNIME_EXECUTABLE', r"C:\KNIME\knime.exe")
KNIME_WORKFLOW_DIR = os.environ.get('KNIME_WORKFLOW_DIR', r"C:\Users\Loadguard\knime-workspace\LoadGuard_Data_Workflow")

# Timezone
TIMEZONE = pytz.timezone(os.environ.get('TIMEZONE', "America/Los_Angeles"))  # PST

# Dataset update time in "HH:MM" 24-hour format
DATASET_UPDATE_TIME = os.environ.get('DATASET_UPDATE_TIME', "23:00")  # Set to desired time

# KNIME workflow schedule time in "HH:MM" 24-hour format
KNIME_WORKFLOW_TIME = os.environ.get('KNIME_WORKFLOW_TIME', "23:30")  # Set to desired time

# Maximum number of retries for KNIME workflow
MAX_KNIME_RETRIES = int(os.environ.get('MAX_KNIME_RETRIES', 5))

# Logging configuration
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'application.log')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Dataset configurations
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

# Dataset URLs
DATASET_URLS = {
    'ActPendInsur': 'https://datahub.transportation.gov/api/views/qh9u-swkp',
    'AuthHist': 'https://data.transportation.gov/api/views/9mw4-x3tu',
    'CarrierAllWithHistory': 'https://data.transportation.gov/api/views/6eyk-hxee',
    'NewCompanyCensusFile': 'https://data.transportation.gov/api/views/az4n-8mr2',
    'VehicleInspectionsFile': 'https://data.transportation.gov/api/views/fx4q-ay7w',
    'InspectionPerUnit': 'https://data.transportation.gov/api/views/wt8s-2hbx',
    'InsurAllWithHistory': 'https://data.transportation.gov/api/views/ypjt-5ydn',
    'CrashFile': 'https://datahub.transportation.gov/api/views/aayw-vxb3'
}

# ZIP files to download
ZIP_FILES = [
    "ftp://ftp.senture.com/",
    "https://ai.fmcsa.dot.gov/SMS/files/"
]

# Dropbox dataset URLs
DROPBOX_DATASETS = [
    'https://www.dropbox.com/scl/fi/rrn5p8ha4x7wd6bb86gwz/CENSUS_PUB_20240509_1of3.csv?rlkey=wc9j8p0ugmb4o0ngoxs1lku6a&st=cbgzc1sb&dl=1',
    'https://www.dropbox.com/scl/fi/hlbew8zt2v7iha72gn5ce/CENSUS_PUB_20240509_2of3.csv?rlkey=siv1rag8c1875t471l8uussnz&st=wwlbggvj&dl=1',
    'https://www.dropbox.com/scl/fi/zj5tznnlmzqrt21jo71f4/CENSUS_PUB_20240509_3of3.csv?rlkey=ld3z7jgsp26ka9d74ryzkpayp&st=farqx7zi&dl=1'
]
