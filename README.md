# LoadGuard KNIME-Socrata Integration System

This project integrates Socrata datasets, FTP sources, and Dropbox files with KNIME workflows, providing automated data updates and processing for transportation-related data.

## Features

- Automated checking and downloading of Socrata datasets
- Handling of FTP and HTTP file downloads
- Integration with Dropbox for additional data sources
- Determination of SMS (Safety Measurement System) latest file
- KNIME workflow execution in batch mode
- Comprehensive error handling and logging
- Scheduled updates and processing

## System Architecture

The system consists of the following main components:

1. **Socrata API**: External data source providing access to various transportation datasets.
2. **FTP and HTTP Sources**: Additional data sources for specific files.
3. **Dropbox Integration**: Fetches census-related datasets.
4. **SMS Updater**: finds the latest Safety Measurement System data updates by probing SMS URL.
5. **File Manager**: Manages local file operations, downloading, and updating dataset files.
6. **KNIME Runner**: Executes KNIME workflows with updated dataset information.
7. **Data Processor**: Orchestrates the entire data processing pipeline.
8. **Error Handler**: Manages errors across all components, ensuring robust error handling and reporting.
9. **Logging System**: Comprehensive logging tracking all operations, updates, and errors.

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure settings in `config/settings.py`
4. Ensure KNIME is installed and the path is correctly set in the configuration (workflow directory needs to be set to the KNIME workspace directory, can not be an exported workflow filee)

## Configuration

Key configuration settings in `config/settings.py`:

- `BASE_DIR`: Base directory of the project
- `DATA_DIR`: Directory to store downloaded data
- `KNIME_EXECUTABLE`: Path to KNIME executable
- `KNIME_WORKFLOW_DIR`: Directory containing KNIME workflows
- `DATASET_UPDATE_INTERVAL`: Interval for checking dataset updates
- `KNIME_WORKFLOW_TIME`: Daily time to run KNIME workflow
- `DATASET_NAMES` and `DATASET_URLS`: List of datasets and their Socrata API URLs
- `ZIP_FILES`: Additional ZIP file sources
- `DROPBOX_DATASETS`: URLs for Dropbox datasets

## Usage

To run the automated update process:

```
python main_scripts/run_update.py
```

To manually run a KNIME workflow: 

```
python main_scripts/knime_runner.py
```

## Project Structure

- `src/`: Core functionality modules
- `config/`: Configuration files
- `main_scripts/`: Execution scripts
- `logs/`: Log files (generated during runtime)
- `data/`: Downloaded datasets (generated during runtime)

## Development

### Running Tests

To run the unit tests:

```
python -m unittest discover tests
```

### Adding New Datasets

To add new datasets to be processed:

1. Update the `DATASETS` list in `config/settings.py`
2. Create corresponding KNIME workflows in the `knime_workflows/` directory
3. Update the main KNIME workflow to handle the new dataset

### Customizing Error Handling

Modify the `src/error_handler.py` file to add new error types or customize error handling behavior.

## Acknowledgments

- Socrata for providing the open data API
- KNIME for their powerful data analysis platform

