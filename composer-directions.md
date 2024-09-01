# KNIME-Socrata Integration Project Setup Instructions

Follow these step-by-step instructions to set up the KNIME-Socrata Integration project.

## 1. Project Structure

Create the following directory structure:

```
knime_socrata_integration/
│
├── src/
│   ├── __init__.py
│   ├── socrata_api.py
│   ├── data_processor.py
│   ├── knime_runner.py
│   ├── file_manager.py
│   └── error_handler.py
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── logging_config.py
├── scripts/
│   ├── run_update.py
│   └── run_knime_workflow.py
├── tests/
│   ├── __init__.py
│   ├── test_socrata_api.py
│   ├── test_data_processor.py
│   └── test_file_manager.py
├── logs/
│   └── .gitkeep
├── data/
│   ├── INSPECTIONS/
│   ├── CENSUS/
│   └── .gitkeep
├── knime_workflows/
│   └── main_workflow.knwf
├── requirements.txt
├── README.md
└── .gitignore
```

## 2. File Creation and Content

Create each file with the following content:

### src/__init__.py

Leave this file empty.

### src/socrata_api.py

```python
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from src.error_handler import APIError

class SocrataAPI:
    def __init__(self, base_url, timeout=10, retries=3):
        self.base_url = base_url
        self.session = requests.Session()
        retry = Retry(total=retries, backoff_factor=0.1)
        self.session.mount('https://', HTTPAdapter(max_retries=retry))
        self.timeout = timeout

    def get_dataset_metadata(self, dataset_id):
        try:
            url = f"{self.base_url}/api/views/{dataset_id}.json"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise APIError(f"Failed to get metadata for dataset {dataset_id}: {str(e)}")

    def download_dataset(self, dataset_id, limit=None):
        try:
            url = f"{self.base_url}/resource/{dataset_id}.csv"
            params = {"$limit": limit} if limit else {}
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise APIError(f"Failed to download dataset {dataset_id}: {str(e)}")
```

### src/data_processor.py

```python
from datetime import datetime
from src.socrata_api import SocrataAPI
from src.file_manager import FileManager
from src.error_handler import ProcessingError
import logging

class DataProcessor:
    def __init__(self, api: SocrataAPI, file_manager: FileManager):
        self.api = api
        self.file_manager = file_manager

    def process_dataset(self, dataset_id, category, last_check_time):
        try:
            metadata = self.api.get_dataset_metadata(dataset_id)
            last_update = datetime.fromtimestamp(metadata['rowsUpdatedAt'])
            
            if last_update > last_check_time:
                data = self.api.download_dataset(dataset_id)
                file_path = self.file_manager.save_dataset(dataset_id, category, data)
                logging.info(f"Updated file for {category}: {file_path}")
                return True
            else:
                logging.info(f"No updates for dataset {dataset_id} in category {category}")
                return False
        except Exception as e:
            raise ProcessingError(f"Error processing dataset {dataset_id} in category {category}: {str(e)}")
```

### src/knime_runner.py

```python
import subprocess
import os
from src.error_handler import KNIMEError

class KNIMERunner:
    def __init__(self, knime_executable, workflow_dir):
        self.knime_executable = knime_executable
        self.workflow_dir = workflow_dir

    def run_workflow(self, workflow_name, params=None):
        try:
            command = [
                self.knime_executable,
                "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
                "-workflowDir", os.path.join(self.workflow_dir, workflow_name),
                "-reset",
                "-nosplash",
                "-nosave",
                "--launcher.suppressErrors",
                "-vmargs", "-Dknime.log.file=logs/knime_execution.log"
            ]
            
            if params:
                for key, value in params.items():
                    command.extend(["-workflow.variable", f"{key},{value},String"])

            result = subprocess.run(command, check=True, capture_output=True, text=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise KNIMEError(f"KNIME workflow execution failed: {e.stderr}")
```

### src/file_manager.py

```python
import os
import shutil
import hashlib
from src.error_handler import FileError

class FileManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def save_dataset(self, dataset_id, category, data):
        try:
            category_dir = os.path.join(self.base_dir, category)
            os.makedirs(category_dir, exist_ok=True)
            
            temp_file = os.path.join(category_dir, f"{dataset_id}_temp.csv")
            final_file = os.path.join(category_dir, f"{dataset_id}_latest.csv")
            
            with open(temp_file, "wb") as f:
                f.write(data)
            
            if not os.path.exists(final_file) or not self._files_are_identical(temp_file, final_file):
                shutil.move(temp_file, final_file)
                return final_file
            else:
                os.remove(temp_file)
                return None
        except Exception as e:
            raise FileError(f"Failed to save dataset {dataset_id}: {str(e)}")

    def _files_are_identical(self, file1, file2):
        return self._get_file_hash(file1) == self._get_file_hash(file2)

    def _get_file_hash(self, file_path):
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
```

### src/error_handler.py

```python
class APIError(Exception):
    pass

class ProcessingError(Exception):
    pass

class KNIMEError(Exception):
    pass

class FileError(Exception):
    pass
```

### config/__init__.py

Leave this file empty.

### config/settings.py

```python
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SOCRATA_BASE_URL = "https://datahub.transportation.gov"
KNIME_EXECUTABLE = "/path/to/knime"
WORKFLOW_DIR = os.path.join(BASE_DIR, "knime_workflows")
DATA_DIR = os.path.join(BASE_DIR, "data")

DATASETS = [
    {"id": "qh9u-swkp", "name": "Primary Dataset", "category": "INSPECTIONS"},
    {"id": "placeholder1", "name": "Dataset 1", "category": "CENSUS"},
    # Add more datasets as needed
]

CHECK_INTERVAL_HOURS = 1
```

### config/logging_config.py

```python
import logging
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, 'logs', 'app.log'),
            'formatter': 'verbose',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['file', 'console'],
        'level': 'INFO',
    },
}

logging.config.dictConfig(LOGGING)
```

### scripts/run_update.py

```python
import schedule
import time
from datetime import datetime, timedelta
from src.socrata_api import SocrataAPI
from src.data_processor import DataProcessor
from src.file_manager import FileManager
from src.knime_runner import KNIMERunner
from config.settings import SOCRATA_BASE_URL, KNIME_EXECUTABLE, WORKFLOW_DIR, DATA_DIR, DATASETS, CHECK_INTERVAL_HOURS
import logging

def main():
    api = SocrataAPI(SOCRATA_BASE_URL)
    file_manager = FileManager(DATA_DIR)
    processor = DataProcessor(api, file_manager)
    knime_runner = KNIMERunner(KNIME_EXECUTABLE, WORKFLOW_DIR)

    def job():
        last_check_time = datetime.now() - timedelta(hours=CHECK_INTERVAL_HOURS)
        updated_datasets = []

        for dataset in DATASETS:
            try:
                if processor.process_dataset(dataset['id'], dataset['category'], last_check_time):
                    updated_datasets.append(dataset['id'])
            except Exception as e:
                logging.error(f"Error processing dataset {dataset['id']}: {str(e)}")

        if updated_datasets:
            try:
                knime_runner.run_workflow("main_workflow.knwf", {"updated_datasets": ",".join(updated_datasets)})
                logging.info("KNIME workflow executed successfully")
            except Exception as e:
                logging.error(f"Error running KNIME workflow: {str(e)}")

    schedule.every(CHECK_INTERVAL_HOURS).hours.do(job)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
```

### scripts/run_knime_workflow.py

```python
from src.knime_runner import KNIMERunner
from config.settings import KNIME_EXECUTABLE, WORKFLOW_DIR
import sys
import logging

def main(workflow_name, params=None):
    knime_runner = KNIMERunner(KNIME_EXECUTABLE, WORKFLOW_DIR)
    try:
        output = knime_runner.run_workflow(workflow_name, params)
        logging.info(f"KNIME workflow {workflow_name} executed successfully")
        logging.info(f"Output: {output}")
    except Exception as e:
        logging.error(f"Error running KNIME workflow {workflow_name}: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_knime_workflow.py  [param1=value1 param2=value2 ...]")
        sys.exit(1)

    workflow_name = sys.argv[1]
    params = dict(arg.split('=') for arg in sys.argv[2:] if '=' in arg)
    main(workflow_name, params)
```

### tests/__init__.py

Leave this file empty.

### tests/test_socrata_api.py

```python
import unittest
from unittest.mock import patch, MagicMock
from src.socrata_api import SocrataAPI
from src.error_handler import APIError

class TestSocrataAPI(unittest.TestCase):

    def setUp(self):
        self.api = SocrataAPI("https://test.socrata.com")

    @patch('src.socrata_api.requests.Session')
    def test_get_dataset_metadata(self, mock_session):
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        mock_session.return_value.get.return_value = mock_response

        result = self.api.get_dataset_metadata("test-id")
        self.assertEqual(result, {"key": "value"})

    @patch('src.socrata_api.requests.Session')
    def test_get_dataset_metadata_error(self, mock_session):
        mock_session.return_value.get.side_effect = Exception("API Error")

        with self.assertRaises(APIError):
            self.api.get_dataset_metadata("test-id")

    @patch('src.socrata_api.requests.Session')
    def test_download_dataset(self, mock_session):
        mock_response = MagicMock()
        mock_response.content = b"test,data\n1,2\n"
        mock_session.return_value.get.return_value = mock_response

        result = self.api.download_dataset("test-id")
        self.assertEqual(result, b"test,data\n1,2\n")

    @patch('src.socrata_api.requests.Session')
    def test_download_dataset_error(self, mock_session):
        mock_session.return_value.get.side_effect = Exception("Download Error")

        with self.assertRaises(APIError):
            self.api.download_dataset("test-id")

if __name__ == '__main__':
    unittest.main()
```

### tests/test_data_processor.py

```python
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.data_processor import DataProcessor
from src.error_handler import ProcessingError

class TestDataProcessor(unittest.TestCase):

    def setUp(self):
        self.mock_api = MagicMock()
        self.mock_file_manager = MagicMock()
        self.processor = DataProcessor(self.mock_api, self.mock_file_manager)

    def test_process_dataset_updated(self):
        self.mock_api.get_dataset_metadata.return_value = {'rowsUpdatedAt': datetime.now().timestamp()}
        self.mock_api.download_dataset.return_value = b"test,data\n1,2\n"
        self.mock_file_manager.save_dataset.return_value = "/path/to/file.csv"

        result = self.processor.process_dataset("test-id", "TEST", datetime(2000, 1, 1))
        self.assertTrue(result)

    def test_process_dataset_not_updated(self):
        self.mock_api.get_dataset_metadata.return_value = {'rowsUpdatedAt': datetime(2000, 1, 1).timestamp()}

        result = self.processor.process_dataset("test-id", "TEST", datetime.now())
        self.assertFalse(result)

    def test_process_dataset_error(self):
        self.mock_api.get_dataset_metadata.side_effect = Exception("API Error")

        with self.assertRaises(ProcessingError):
            self.processor.process_dataset("test-id", "TEST", datetime.now())

if __name__ == '__main__':
    unittest.main()
```

### tests/test_file_manager.py

```python
import unittest
import tempfile
import os
from src.file_manager import FileManager
from src.error_handler import FileError

class TestFileManager(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_manager = FileManager(self.temp_dir)

    def tearDown(self):
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.temp_dir)

    def test_save_dataset(self):
        data = b"test,data\n1,2\n"
        file_path = self.file_manager.save_dataset("test-id", "TEST", data)
        
        self.assertIsNotNone(file_path)
        self.assertTrue(os.path.exists(file_path))
        with open(file_path, 'rb') as f:
            self.assertEqual(f.read(), data)

    def test_save_dataset_no_change(self):
        data = b"test,data\n1,2\n"
        self.file_manager.save_dataset("test-id", "TEST", data)
        
        # Try to save the same data again
        file_path = self.file_manager.save_dataset("test-id", "TEST", data)
        self.assertIsNone(file_path)

    def test_save_dataset_error(self):
        with patch('src.file_manager.open', side_effect=IOError("Write error")):
            with self.assertRaises(FileError):
                self.file_manager.save_dataset("test-id", "TEST", b"data")

if __name__ == '__main__':
    unittest.main()
```

### requirements.txt

Create this file with the following content:

```
requests==2.26.0
schedule==1.1.0
```

### README.md

Create this file with the following content:

```markdown
# KNIME-Socrata Integration

This project integrates Socrata datasets with KNIME workflows, providing automated data updates and processing.

## Features

- Automated checking and downloading of Socrata datasets
- File integrity checks and efficient storage
- KNIME workflow execution in batch mode
- Error handling and comprehensive logging

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure settings in `config/settings.py`
4. Ensure KNIME is installed and the path is correctly set in the configuration

## Usage

To run the automated update process:

```
python scripts/run_update.py
```

To manually run a KNIME workflow:

```
python scripts/run_knime_workflow.py <workflow_name> [param1=value1 param2=value2 ...]
```

## Project Structure

- `src/`: Core functionality modules
- `config/`: Configuration files
- `scripts/`: Execution scripts
- `tests/`: Unit tests
- `logs/`: Log files (generated during runtime)
- `data/`: Downloaded datasets (generated during runtime)
- `knime_workflows/`: KNIME workflow files

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
```

### .gitignore

Create this file with the following content:

```
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# PyInstaller
#  Usually these files are written by a python script from a template
#  before PyInstaller builds the exe, so as to inject date/other infos into it.
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
.hypothesis/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
target/

# Jupyter Notebook
.ipynb_checkpoints

# pyenv
.python-version

# celery beat schedule file
celerybeat-schedule

# SageMath parsed files
*.sage.py

# Environments
.env
.venv
env/
venv/
ENV/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/

# KNIME
*.knwf

# Project specific
logs/*
!logs/.gitkeep
data/*
!data/.gitkeep
```

## Final Steps

1. Create empty directories:
   - Create an empty `logs/` directory with a `.gitkeep` file inside.
   - Create an empty `data/` directory with a `.gitkeep` file inside.
   - Create empty `INSPECTIONS/` and `CENSUS/` directories inside the `data/` directory.

2. Place your KNIME workflow file (e.g., `main_workflow.knwf`) in the `knime_workflows/` directory.

3. Adjust the `KNIME_EXECUTABLE` path in `config/settings.py` to match your KNIME installation.

4. Review and adjust other settings in `config/settings.py` as needed for your specific use case.

5. Run the tests to ensure everything is set up correctly:
   ```
   python -m unittest discover tests
   ```

6. You're now ready to use the KNIME-Socrata Integration system!

Remember to update the README.md with any specific instructions or information relevant to your implementation of the system.