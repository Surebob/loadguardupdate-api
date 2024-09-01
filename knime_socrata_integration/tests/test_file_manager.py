import unittest
import tempfile
import os
from src.file_manager import FileManager
from src.error_handler import FileError

from unittest.mock import patch

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