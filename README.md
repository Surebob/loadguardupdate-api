# 🚛 LoadGuard KNIME-Socrata Integration System 🚛

Welcome to the LoadGuard KNIME-Socrata Integration System! This robust automation platform seamlessly integrates transportation datasets from Socrata, FTP servers, and Dropbox with KNIME workflows. It provides an end-to-end solution for automated data updates, processing, and analysis tailored for LoadGuard's internal data needs.

## 🎯 Key Features

- **Automated Data Retrieval**: Effortlessly check and download the latest datasets from multiple sources.
- **Seamless Integration**: Harmonize data from Socrata APIs, FTP servers, and Dropbox into unified workflows.
- **KNIME Workflow Automation**: Execute complex KNIME workflows in batch mode without manual intervention.
- **Advanced Error Handling**: Robust error detection and reporting across all components.
- **Comprehensive Logging**: Detailed logs for monitoring operations, updates, and errors.
- **Flexible Scheduling**: Schedule dataset updates and workflow executions at specific times.
- **Scalable Architecture**: Easily add new data sources and workflows as our needs grow.

## 🏗️ System Architecture

The system is built with a modular architecture, consisting of the following core components:

1. Socrata API Module: Interfaces with Socrata to access various transportation datasets.
2. FTP/HTTP Handler: Manages the downloading of files from FTP and HTTP sources.
3. Dropbox Handler: Integrates Dropbox datasets, particularly for census-related data.
4. SMS Updater: Automatically identifies and downloads the latest Safety Measurement System (SMS) data.
5. File Manager: Handles all file operations, including downloading and updating datasets.
6. KNIME Runner: Executes KNIME workflows using the latest data, running in batch mode.
7. Scheduler: Utilizes APScheduler for precise scheduling of updates and workflows.
8. Error Handler: Centralized error management across all modules.
9. Logging System: Provides real-time logging and monitoring of system activities.

## 🚀 Getting Started

### Prerequisites

- Python 3.11+: Ensure you have Python installed.
- KNIME Analytics Platform: Install KNIME.

### Installation Steps

1. Clone the Repository

```bash
git clone https://github.com/Load-Guard/LoadGuard-KNIME-Socrata-Integration-System.git
```

2. Navigate to the Project Directory

```bash
cd LoadGuard-KNIME-Socrata-Integration-System
```

3. Create a Virtual Environment (Recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows use venv\Scripts\activate
```

4. Install Dependencies

```bash
pip install -r requirements.txt
```

5. Configure Settings

- Open `config/settings.py` and update the configurations as per your environment.
- Ensure that `KNIME_EXECUTABLE` and `KNIME_WORKFLOW_DIR` paths are correctly set.

6. Set Up KNIME Workflows

- Place your KNIME workflows in the directory specified by `KNIME_WORKFLOW_DIR`.
- Ensure that the workflows are configured to run in batch mode.

## ⚙️ Configuration Overview

Key settings in `config/settings.py`:

### General Settings

- `BASE_DIR`: Base directory of the project.
- `DATA_DIR`: Directory to store downloaded data.
- `TIMEZONE`: Timezone for scheduling tasks.

### KNIME Settings

- `KNIME_EXECUTABLE`: Path to the KNIME executable.
- `KNIME_WORKFLOW_DIR`: Directory containing KNIME workflows.
- `MAX_KNIME_RETRIES`: Maximum number of retries for KNIME workflows.

### Scheduling Settings

- `DATASET_UPDATE_TIME`: Time to check and update datasets (e.g., "23:40").
- `KNIME_WORKFLOW_TIME`: Time to run KNIME workflows (e.g., "23:45").

### Datasets

- `DATASET_NAMES`: List of dataset names.
- `DATASET_URLS`: Mapping of dataset names to their Socrata API URLs.
- `ZIP_FILES`: List of FTP/HTTP sources for ZIP files.
- `DROPBOX_DATASETS`: List of Dropbox dataset URLs.

## 📖 Usage Guide

### Running the Automated Update Process

```bash
python main_scripts/run_update.py
```

Description: Initiates the update process, which checks for dataset updates, downloads new data, and runs KNIME workflows according to the schedule.

### Manually Running a KNIME Workflow

```bash
python main_scripts/knime_runner.py
```

Description: Executes the KNIME workflow immediately, independent of the scheduled times.

## 🗂️ Project Structure

```
LoadGuard-KNIME-Socrata-Integration-System/
├── config/
│   └── settings.py          # Configuration settings
├── data/                    # Directory for downloaded datasets
├── logs/                    # Logs generated during runtime
├── main_scripts/
│   ├── run_update.py        # Main script for running updates
│   └── knime_runner.py      # Script to run KNIME workflows
├── src/                     # Core modules
│   ├── socrata_api.py
│   ├── zip_file_handler.py
│   ├── dropbox_handler.py
│   ├── sms_update.py
│   ├── file_manager.py
│   ├── error_handler.py
│   └── logging_config.py
├── tests/                   # Unit tests
├── requirements.txt         # Python dependencies
└── README.md                # Project documentation
```

## 🛠️ Development Guidelines

### Adding New Datasets

1. Update Configuration
   - Add the new dataset to `DATASET_NAMES` and `DATASET_URLS` in `config/settings.py`.

2. Modify KNIME Workflows
   - Create or update KNIME workflows to process the new dataset.
   - Ensure the workflow directory is updated in `KNIME_WORKFLOW_DIR`.

3. Test the Integration
   - Run the update process and monitor logs to ensure the new dataset is handled correctly.

### Customizing Error Handling

- Error Types: Define new error types in `src/error_handler.py`.
- Handling Logic: Implement custom handling logic in modules to respond to specific errors.

### Logging Enhancements

- Adjust logging levels and formats in `src/logging_config.py`.
- Implement additional logging in modules where detailed tracing is beneficial.

## 📊 Data Sources and Datasets

The system integrates multiple datasets crucial for LoadGuard's operations:

- ActPendInsur: Active Pending Insurance data.
- AuthHist: Authority History.
- CarrierAllWithHistory: Comprehensive carrier data with historical records.
- NewCompanyCensusFile: Latest census information for new companies.
- VehicleInspectionsFile: Data on vehicle inspections.
- InspectionPerUnit: Inspection data per unit.
- InsurAllWithHistory: Insurance data with historical context.
- CrashFile: Records of transportation-related crashes.

### Support and Maintenance

- Monitoring: Regularly check the `logs/` directory to monitor the system's health and performance.
- Updates: Keep dependencies up to date. Test thoroughly before deploying updates to the production environment.

## 🧭 Roadmap

- Dockerization: Containerize the application for consistent deployment across environments.
- Web Interface: Develop an internal dashboard for monitoring and controlling the system.
- Enhanced Error Reporting: Integrate with internal alerting systems for real-time error notifications.
- Data Visualization: Incorporate data visualization tools for immediate insights post-processing.

## 💡 Tips and Best Practices

- Use Virtual Environments: This helps manage dependencies and avoid conflicts.
- Regular Backups: Keep backups of your `data/` directory to prevent data loss.
- Stay Updated: Regularly run `pip install --upgrade -r requirements.txt` to keep dependencies current.
- Documentation: Keep this README and other documentation up to date with any changes made to the system.