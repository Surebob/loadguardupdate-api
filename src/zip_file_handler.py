import os
import requests
from ftplib import FTP
from urllib.parse import urlparse
from datetime import datetime
import logging

class ZipFileHandler:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logger = logging.getLogger(__name__)

    def check_and_download(self, url):
        parsed_url = urlparse(url)
        remote_filename = os.path.basename(parsed_url.path)
        file_dir = os.path.join(self.base_dir, os.path.splitext(remote_filename)[0])
        os.makedirs(file_dir, exist_ok=True)
        local_path = os.path.join(file_dir, remote_filename)

        remote_date = self._extract_date_from_filename(remote_filename)

        if os.path.exists(local_path):
            local_filename = os.path.basename(local_path)
            local_date = self._extract_date_from_filename(local_filename)
            if remote_date <= local_date:
                self.logger.info(f"No update needed for {remote_filename}")
                return False

        self.logger.info(f"Newer version available for {remote_filename}")
        if parsed_url.scheme == 'ftp':
            self._download_ftp(parsed_url, local_path)
        elif parsed_url.scheme in ['http', 'https']:
            self._download_http(url, local_path)
        self.logger.info(f"Downloaded {remote_filename}")
        return True

    def _extract_date_from_filename(self, filename):
        # Extract date from filename (assuming format like 'YYYYMM' or 'YYYYMMM')
        date_str = ''.join(filter(str.isdigit, filename))
        if len(date_str) == 6:
            return datetime.strptime(date_str, '%Y%m')
        elif len(date_str) == 4:
            return datetime.strptime(date_str, '%Y')
        else:
            raise ValueError(f"Unexpected date format in filename: {filename}")

    def _download_ftp(self, parsed_url, local_path):
        with FTP(parsed_url.netloc) as ftp:
            ftp.login()
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {parsed_url.path}', f.write)

    def _download_http(self, url, local_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(response.content)