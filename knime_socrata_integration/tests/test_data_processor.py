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