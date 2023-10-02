import logging
from os import getenv
from typing import List

from kami_logging import benchmark_with, logging_with

tiny_api_logger = logging.getLogger('Tiny API')

import requests
from dotenv import load_dotenv

load_dotenv()


class TinyAPI:
    def __init__(self, base_url: str = 'https://api.tiny.com.br/api2/'):
        self.base_url = base_url
        self.token = None

    def _set_token(self) -> bool:
        try:
            self.token = getenv('TINY_API_TOKEN')
            if not self.token:
                raise EnvironmentError(
                    'Environment variable TINY_API_TOKEN not found.'
                )
            return True
        except Exception as err:
            raise Exception(f'An unknown error occurred: {err}') from err

    @benchmark_with(tiny_api_logger)
    @logging_with(tiny_api_logger)
    def get_product_by_sku(self, sku: str) -> dict:
        if self.token is None:
            self._set_token()
        endpoint_url = f'{self.base_url}produtos.pesquisa.php'
        data = {'token': self.token, 'pesquisa': sku, 'formato': 'JSON'}
        tiny_api_logger.info(f'{endpoint_url} - {data}')
        try:
            res = requests.post(endpoint_url, data=data)
            res.raise_for_status()
            response = res.json()
            if (
                'retorno' in response
                and response['retorno']['status'] == 'Erro'
            ):
                raise Exception(
                    f"HTTP error occurred: {response['retorno']['erros']}"
                )
            return response
        except requests.exceptions.HTTPError as err:
            raise Exception(f'HTTP error occurred: {err}') from err
        except requests.exceptions.RequestException as err:
            raise Exception(f'An error occurred: {err}') from err
        except Exception as err:
            raise Exception(f'An unknown error occurred: {err}') from err

    @benchmark_with(tiny_api_logger)
    @logging_with(tiny_api_logger)
    def get_products_list_by_sku(self, sku_list) -> List[str]:
        products_list = List[str]
        try:
            for sku in sku_list:
                product_dict = {}
                try:
                    product_data = self.get_product_by_sku(sku)
                    product_dict = product_data
                except Exception as err:
                    product_dict = {'sku': sku, 'error': str(err)}
                finally:
                    products_list.append(product_dict)
        except Exception as err:
            raise Exception(f'An unknown error occurred: {err}') from err
        finally:
            return products_list
