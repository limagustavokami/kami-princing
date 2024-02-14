import json
import unittest
from unittest.mock import MagicMock, mock_open, patch

from kami_pricing.api.tiny import TinyAPI, TinyAPIError


class TestTinyAPI(unittest.TestCase):
    def setUp(self):
        self.tiny_api = TinyAPI()
        self.tiny_api.credentials_path = 'dummy/path/to/credentials.json'

    @patch('builtins.open', mock_open(read_data='{"token": "your_token"}'))
    @patch('json.load')
    def test_set_credentials_success(self, mock_json_load):
        mock_json_load.return_value = {'token': 'your_token'}

        try:
            self.tiny_api._set_credentials()
            self.assertEqual(
                self.tiny_api.credentials, {'token': 'your_token'}
            )
        except TinyAPIError:
            self.fail('TinyAPIError was raised unexpectedly!')

    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_set_credentials_file_not_found(self, mock_file_not_found_error):
        with self.assertRaises(TinyAPIError) as context:
            self.tiny_api._set_credentials()
        self.assertIn('Credentials file not found', str(context.exception))

    @patch('builtins.open', side_effect=PermissionError())
    def test_set_credentials_permission_error(self, mock_permission_error):
        with self.assertRaises(TinyAPIError) as context:
            self.tiny_api._set_credentials()
        self.assertIn(
            'No permission to read credentials file', str(context.exception)
        )

    @patch('builtins.open', mock_open(read_data='{"token": "your_token"'))
    @patch(
        'json.load',
        side_effect=json.JSONDecodeError('Invalid JSON', doc='', pos=0),
    )
    def test_set_credentials_json_decode_error(self, mock_json_load_error):
        with self.assertRaises(TinyAPIError) as context:
            self.tiny_api._set_credentials()
        self.assertIn('contains invalid JSON', str(context.exception))

    @patch('builtins.open', side_effect=Exception('Generic error'))
    def test_set_credentials_generic_error(self, mock_generic_error):
        with self.assertRaises(TinyAPIError) as context:
            self.tiny_api._set_credentials()
        self.assertIn(
            'Failed to get credentials: Generic error', str(context.exception)
        )


if __name__ == '__main__':
    unittest.main()
