import json
import logging
from os import path
from typing import Dict, List, Union

import httpx
import pandas as pd
import requests
from kami_logging import benchmark_with, logging_with

from kami_pricing.constant import ROOT_DIR

plugg_to_api_logger = logging.getLogger('Anymarket API')
base_url: str = 'https://api.plugg.to'
plugg_to_credentials_path: str = path.join(
    ROOT_DIR, 'credentials/plugg_to.json'
)


class PluggToAPIError(Exception):
    pass


class PluggToAPI:
    def __init__(
        self,
        base_url: str = base_url,
        credentials_path: str = plugg_to_credentials_path,
    ):
        self.base_url = base_url
        self.credentials_path = credentials_path
        self.credentials = None
        self.result = None

    @benchmark_with(plugg_to_api_logger)
    @logging_with(plugg_to_api_logger)
    def _set_credentials(self):
        try:
            with open(self.credentials_path, 'r') as f:
                self.credentials = json.load(f)
        except FileNotFoundError:
            raise PluggToAPIError(
                f'Credentials file not found at {self.credentials_path}.'
            )
        except PermissionError:
            raise PluggToAPIError(
                f'No permission to read credentials file at {self.credentials_path}.'
            )
        except json.JSONDecodeError:
            raise PluggToAPIError(
                f'The credentials file at {self.credentials_path} contains invalid JSON.'
            )
        except Exception as e:
            raise PluggToAPIError(f'Failed to get credentials: {e}')

    def connect(self) -> str:
        try:
            with open(self.credentials_path, 'r') as file:
                credentials = json.load(file)
            payload = {
                'client_id': credentials['client_id'],
                'client_secret': credentials['client_secret'],
                'username': credentials['username'],
                'password': credentials['password'],
                'grant_type': 'password',
            }
            url = f'{self.base_url}/oauth/token'
            headers = {
                'accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
            return response.json()['access_token']
        except Exception as e:
            raise Exception(f'Failed to connect: {e}')

    def _set_access_token(self):
        try:
            self.access_token = self.connect()
        except Exception as e:
            raise Exception(f'Failed to set access token: {e}')

    def update_special_price(self, sku: str, price: Union[float, int]) -> None:
        try:
            headers = {
                'accept': 'application/json',
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
            }
            url = f'{self.base_url}/skus/{sku}'
            data = {'special_price': price}
            response = requests.put(url, headers=headers, json=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f'Failed to update special price: {e}')
        except Exception as e:
            raise Exception(f'An error occurred: {e}')
