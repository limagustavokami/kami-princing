import json
import logging
from os import path
from typing import Dict, List

import httpx
from httpx import Response
from kami_logging import benchmark_with, logging_with
from requests.exceptions import HTTPError, RequestException

from kami_pricing.constant import ROOT_DIR

tiny_api_logger = logging.getLogger('Tiny API')
base_url = 'https://api.tiny.com.br/api2/'
tiny_credentials_path = path.join(ROOT_DIR, 'credentials/tiny.json')


class TinyAPIError(Exception):
    pass


class TinyAPI:
    def __init__(
        self,
        base_url: str = base_url,
        credentials_path: str = tiny_credentials_path,
    ):
        self.base_url = base_url
        self.credentials_path = credentials_path
        self.credentials = None
        self.result = None

    @benchmark_with(tiny_api_logger)
    @logging_with(tiny_api_logger)
    def _set_credentials(self):
        try:
            with open(self.credentials_path, 'r') as f:
                self.credentials = json.load(f)
        except FileNotFoundError:
            raise TinyAPIError(
                f'Credentials file not found at {self.credentials_path}.'
            )
        except PermissionError:
            raise TinyAPIError(
                f'No permission to read credentials file at {self.credentials_path}.'
            )
        except json.JSONDecodeError:
            raise TinyAPIError(
                f'The credentials file at {self.credentials_path} contains invalid JSON.'
            )
        except Exception as e:
            raise TinyAPIError(f'Failed to get credentials: {e}')

    @benchmark_with(tiny_api_logger)
    @logging_with(tiny_api_logger)
    def _connect(
        self,
        method: str = 'POST',
        endpoint: str = '',
        payload: List = [],
        headers: Dict = {},
        query: str = '',
    ):
        try:
            if not self.credentials:
                self._set_credentials()

            payload = {
                'token': self.credentials['token'],
                'pesquisa': query,
                'formato': 'JSON',
            }

            method = method.upper()

            with httpx.Client() as client:
                response = {
                    'GET': lambda: client.get(
                        base_url + endpoint, headers=headers
                    ),
                    'POST': lambda: client.post(
                        base_url + endpoint, json=payload, headers=headers
                    ),
                    'PUT': lambda: client.put(
                        base_url + endpoint, json=payload, headers=headers
                    ),
                    'DELETE': lambda: client.delete(
                        base_url + endpoint, headers=headers
                    ),
                    'PATCH': lambda: client.patch(
                        base_url + endpoint, json=payload, headers=headers
                    ),
                }.get(method, lambda: None)()

                if response is None:
                    raise ValueError(f'Unsupported HTTP method: {method}')

                response.raise_for_status()
                self.result = response.json()

        except httpx.HTTPStatusError as e:
            raise TinyAPIError(f'HTTP error occurred: {e}')
        except httpx.RequestError as e:
            raise TinyAPIError(f'Failed to connect: {e}')
        except ValueError as e:
            raise TinyAPIError(str(e))
        except Exception as e:
            raise TinyAPIError(f'Failed to connect: {e}')

    @benchmark_with(tiny_api_logger)
    @logging_with(tiny_api_logger)
    def get_product_by_sku(self, sku: str) -> Dict:
        endpoint = 'produtos.pesquisa.php'
        try:
            response = self._connect(endpoint=endpoint, query=sku)
            if 'retorno' in response and response['retorno']['status'] == 'OK':
                product_dict = response['retorno']['produtos'][0]['produto']
                return product_dict
            elif (
                'retorno' in response
                and response['retorno']['status'] == 'Erro'
            ):
                raise TinyAPIError(
                    f"Tiny API Raises: { response['retorno']['erros']}"
                )
        except HTTPError as e:
            raise HTTPError(f'HTTP error occurred: {e}')
        except RequestException as e:
            raise RequestException(f'An request error occurred: {e}')

    @benchmark_with(tiny_api_logger)
    @logging_with(tiny_api_logger)
    def get_products_list_by_sku(self, sku_list: List[str]) -> List[dict]:
        products_list = []
        try:
            for sku in sku_list:
                product_dict = {}
                try:
                    product_data = self.get_product_by_sku(sku)
                    product_dict = product_data
                except Exception as e:
                    product_dict = {'sku': sku, 'error': str(e)}
                finally:
                    products_list.append(product_dict)
        except Exception as e:
            raise TinyAPIError(f'An unknown error occurred: {e}')
        finally:
            return products_list
