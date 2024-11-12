from pydantic_settings import BaseSettings
from typing import Dict
import os
import pytz

class Settings(BaseSettings):
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = True

    # Base Paths
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR: str = os.path.join(BASE_DIR, "data")

    # Timezone and Logging
    TIMEZONE: str = "America/Los_Angeles"
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = os.path.join(BASE_DIR, "logs", "application.log")

    # Schedule Times (24-hour format)
    DATASET_UPDATE_TIME: str = "22:00"
    CLICKER_SCHEDULE_TIME: str = "23:45"

    # URLs
    FTP_URL: str = "ftp://ftp.senture.com/"
    SMS_BASE_URL: str = "https://ai.fmcsa.dot.gov/SMS/files/"

    # Dataset URLs
    DATASET_URLS: Dict[str, str] = {
        'ActPendInsur': 'https://datahub.transportation.gov/api/views/qh9u-swkp',
        'AuthHist': 'https://data.transportation.gov/api/views/9mw4-x3tu',
        'CarrierAllWithHistory': 'https://data.transportation.gov/api/views/6eyk-hxee',
        'NewCompanyCensusFile': 'https://data.transportation.gov/api/views/az4n-8mr2',
        'VehicleInspectionsFile': 'https://data.transportation.gov/api/views/fx4q-ay7w',
        'InspectionPerUnit': 'https://data.transportation.gov/api/views/wt8s-2hbx',
        'InsurAllWithHistory': 'https://data.transportation.gov/api/views/ypjt-5ydn',
        'CrashFile': 'https://datahub.transportation.gov/api/views/aayw-vxb3'
    }

    # Webhook Settings
    WEBHOOK_TIMEOUT: int = 5
    WEBHOOK_RETRY_COUNT: int = 3

    # Database
    DATABASE_URL: str = "sqlite:///./sql_app.db"

    class Config:
        env_file = ".env"

settings = Settings() 