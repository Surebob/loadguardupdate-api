# config/settings.py

import os
import pytz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# KNIME settings
KNIME_EXECUTABLE = os.environ.get('KNIME_EXECUTABLE', r"C:\KNIME\knime.exe")
KNIME_WORKFLOW_DIR = os.environ.get('KNIME_WORKFLOW_DIR', r"C:\Users\Loadguard\knime-workspace\Test1")

# Timezone
TIMEZONE = pytz.timezone(os.environ.get('TIMEZONE', "America/Los_Angeles"))  # PST

# Dataset update times in "HH:MM" 24-hour format
DATASET_UPDATE_TIME = os.environ.get('DATASET_UPDATE_TIME', "22:00")  # 11:00 PM
KNIME_WORKFLOW_TIME = os.environ.get('KNIME_WORKFLOW_TIME', "23:45")  # 11:45 PM

# Maximum number of retries for KNIME workflow
MAX_KNIME_RETRIES = int(os.environ.get('MAX_KNIME_RETRIES', 5))

# Logging configuration
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'application.log')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Dataset URLs for Socrata datasets
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

# FTP and SMS URLs
FTP_URL = 'ftp://ftp.senture.com/'
SMS_BASE_URL = 'https://ai.fmcsa.dot.gov/SMS/files/'