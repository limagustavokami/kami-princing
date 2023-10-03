import unittest
from unittest.mock import MagicMock, patch
from requests.exceptions import HTTPError, RequestException
from kami_princing.tiny_api import TinyAPI


class TestTinyAPI(unittest.TestCase):
    @patch('requests.post')
    def test_get_product_by_sku_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'product': 'data'}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'
        response = tiny_api.get_product_by_sku('some_sku')
        self.assertEqual(response, {'product': 'data'})

    @patch('requests.post')
    def test_get_product_by_sku_http_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError(
            'HTTP error occurred: '
        )
        mock_post.return_value = mock_response

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'
        with self.assertRaises(Exception):
            tiny_api.get_product_by_sku('some_sku')

    @patch('requests.post')
    def test_get_products_list_by_sku(
        self, mock_get_product_by_sku, mock_post
    ):
        mock_get_product_by_sku.side_effect = [
            {'product': 'data1'},
            Exception('Product not found'),
            {'product': 'data2'},
        ]

        tiny_api = TinyAPI()
        tiny_api.token = 'fake_token'
        sku_list = ['sku1', 'sku2', 'sku3']
        response = tiny_api.get_products_list_by_sku(sku_list)

        self.assertEqual(
            response,
            [
                {'product': 'data1'},
                {'sku': 'sku2', 'error': 'Product not found'},
                {'product': 'data2'},
            ],
        )


if __name__ == '__main__':
    unittest.main()
