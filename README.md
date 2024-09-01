# KNIME-Socrata Integration

This project integrates Socrata datasets with KNIME workflows, providing automated data updates and processing.

## Features

- Automated checking and downloading of Socrata datasets
- File integrity checks and efficient storage
- KNIME workflow execution in batch mode
- Error handling and comprehensive logging

## System Architecture

![KNIME-Socrata Integration System](https://github.com/Load-Guard/LoadGuard-KNIME-Socrata-Integration-System/blob/main/assets/LoadGuard_Knime.png)

The system consists of the following main components:

1. **Socrata API**: External data source providing access to various datasets.
3. **DataProcessor**: Orchestrates data processing, determining if datasets need updating.
4. **FileManager**: Manages local file operations, saving and updating dataset files.
5. **KNIME Runner**: Executes KNIME workflows with updated dataset information.
6. **KNIME Workflow**: Processes the updated data, performing analysis and transformations.
7. **run_update.py**: Scheduled script that checks for updates and triggers the data processing pipeline.
8. **run_knime_workflow.py**: Script for manual execution of KNIME workflows with custom parameters.
9. **Error Handler**: Manages errors across all components, ensuring robust error handling and reporting.
10. **Logging**: Comprehensive logging system tracking all operations, updates, and errors.

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
python scripts/run_knime_workflow.py
```

## Project Structure

- `src/`: Core functionality modules
- `config/`: Configuration files
- `scripts/`: Execution scripts
- `tests/`: Unit tests
- `logs/`: Log files (generated during runtime)
- `data/`: Downloaded datasets (generated during runtime)
- `knime_workflows/`: KNIME workflow files

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

