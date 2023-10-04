import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import HTTPError

from kami_princing.tiny_api import TinyAPI, TinyAPIError


class TestTinyAPI(unittest.TestCase):
    @patch('requests.post')
    def test_get_product_by_sku_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'retorno': {
                'status': 'OK',
                'produtos': [{'produto': {'product': 'data1'}}],
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'

        response = tiny_api.get_product_by_sku('some_sku')

        self.assertEqual(response, {'product': 'data1'})

    @patch('requests.post')
    def test_get_product_by_sku_http_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'retorno': {'status': 'Erro', 'erros': 'Some error message'}
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'

        with self.assertRaises(TinyAPIError) as context:
            tiny_api.get_product_by_sku('some_sku')

        self.assertIn(
            'Tiny API Raises: Some error message', str(context.exception)
        )

    @patch('requests.post')
    def test_get_products_list_by_sku_success(self, mock_post):
        mock_response1 = MagicMock()
        mock_response1.json.return_value = {
            'retorno': {
                'status': 'OK',
                'produtos': [{'produto': {'product': 'data1'}}],
            }
        }
        mock_response1.raise_for_status.return_value = None

        mock_response2 = MagicMock()
        mock_response2.json.return_value = {
            'retorno': {
                'status': 'OK',
                'produtos': [{'produto': {'product': 'data2'}}],
            }
        }
        mock_response2.raise_for_status.return_value = None

        mock_post.side_effect = [mock_response1, mock_response2]

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'
        response = tiny_api.get_products_list_by_sku(['sku1', 'sku2'])

        self.assertEqual(
            response, [{'product': 'data1'}, {'product': 'data2'}]
        )

    @patch('requests.post')
    def test_get_products_list_by_sku_http_error(self, mock_post):
        mock_response_success = MagicMock()
        mock_response_success.json.return_value = {
            'retorno': {
                'status': 'OK',
                'produtos': [{'produto': {'product': 'data1'}}],
            }
        }
        mock_response_success.raise_for_status.return_value = None

        mock_response_error = MagicMock()
        mock_response_error.raise_for_status.side_effect = HTTPError(
            'HTTP error occurred: '
        )

        mock_post.side_effect = [mock_response_success, mock_response_error]

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'

        response = tiny_api.get_products_list_by_sku(['sku1', 'sku2'])

        self.assertEqual(response[0], {'product': 'data1'})
        self.assertEqual(response[1]['sku'], 'sku2')
        self.assertIn('HTTP error', response[1]['error'])


if __name__ == '__main__':
    unittest.main()
