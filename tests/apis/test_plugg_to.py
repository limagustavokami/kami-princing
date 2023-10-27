import json
import unittest
from unittest.mock import mock_open, patch

# Using the provided names for module and class
from kami_pricing.api.plugg_to import PluggToAPI, PluggToAPIError


class TestPluggToAPI(unittest.TestCase):
    def setUp(self):
        self.api_instance = PluggToAPI()
        self.api_instance.credentials_path = 'dummy/path/to/credentials.json'

    # Success Test
    @patch('builtins.open', mock_open(read_data='{"token": "your_token"}'))
    @patch('json.load')
    def test_set_credentials_success(self, mock_json_load):
        mock_json_load.return_value = {'token': 'your_token'}

        try:
            self.api_instance._set_credentials()
            self.assertEqual(
                self.api_instance.credentials, {'token': 'your_token'}
            )
        except PluggToAPIError:
            self.fail('PluggToAPIError was raised unexpectedly!')

    # FileNotFoundError Test
    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_set_credentials_file_not_found(self, mock_error):
        with self.assertRaises(PluggToAPIError) as context:
            self.api_instance._set_credentials()
        self.assertIn('Credentials file not found', str(context.exception))

    # PermissionError Test
    @patch('builtins.open', side_effect=PermissionError())
    def test_set_credentials_permission_error(self, mock_permission_error):
        with self.assertRaises(PluggToAPIError) as context:
            self.api_instance._set_credentials()
        self.assertIn(
            'No permission to read credentials file', str(context.exception)
        )

    # JSONDecodeError Test
    @patch('builtins.open', mock_open(read_data='{"token": "your_token"'))
    @patch(
        'json.load',
        side_effect=json.JSONDecodeError('Invalid JSON', doc='', pos=0),
    )
    def test_set_credentials_json_decode_error(self, mock_json_load):
        with self.assertRaises(PluggToAPIError) as context:
            self.api_instance._set_credentials()
        self.assertIn('contains invalid JSON', str(context.exception))

    # Generic Exception Test
    @patch('builtins.open', side_effect=Exception('Generic error'))
    def test_set_credentials_generic_error(self, mock_error):
        with self.assertRaises(PluggToAPIError) as context:
            self.api_instance._set_credentials()
        self.assertIn(
            'Failed to get credentials: Generic error', str(context.exception)
        )


if __name__ == '__main__':
    unittest.main()
