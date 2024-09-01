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