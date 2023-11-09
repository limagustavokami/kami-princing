import json
import logging
from os import path
from typing import Dict, List

import httpx
import pandas as pd
from kami_logging import benchmark_with, logging_with

from kami_pricing.constant import ROOT_DIR

plugg_to_api_logger = logging.getLogger('PluggTo API')
base_url: str = 'https://api.plugg.to'
plugg_to_credentials_path: str = path.join(
    ROOT_DIR, 'credentials/plugg_to_hairpro.json'
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
        self.access_token = None
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
    
    @benchmark_with(plugg_to_api_logger)
    @logging_with(plugg_to_api_logger)
    def _set_access_token(self):
        try:
            if not self.credentials:
                self._set_credentials()

            headers = {
                'accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            payload = {
                'client_id': self.credentials['client_id'],
                'client_secret': self.credentials['client_secret'],
                'username': self.credentials['username'],
                'password': self.credentials['password'],
                'grant_type': 'password',
            }
            with httpx.Client() as client:
                response = client.post(
                    f'{self.base_url}/oauth/token',
                    data=payload,
                    headers=headers
                )
                response.raise_for_status()
                self.access_token = response.json()['access_token']
        except Exception as e:
            raise Exception(f'Failed to set access token: {e}')

    def connect(
        self,
        method: str = 'GET',
        endpoint: str = '',
        payload: List = [],
        headers: Dict = {},              
    ):
        try:
            if not self.access_token:
                self._set_access_token()
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
            if 'accept' not in headers:
                headers['accept'] = 'application/json'
            if 'Authorization' not in headers:
                headers['Authorization'] = f'Bearer {self.access_token}'
                
            method = method.upper()

            with httpx.Client() as client:
                response = {
                    'GET': lambda: client.get(
                        self.base_url + endpoint, headers=headers
                    ),
                    'POST': lambda: client.post(
                        self.base_url + endpoint, json=payload, headers=headers
                    ),
                    'PUT': lambda: client.put(
                        self.base_url + endpoint, data=payload, headers=headers
                    ),
                    'DELETE': lambda: client.delete(
                        self.base_url + endpoint, headers=headers
                    ),
                    'PATCH': lambda: client.patch(
                        self.base_url + endpoint, json=payload, headers=headers
                    ),
                }.get(method, lambda: None)()

                if response is None:
                    raise ValueError(f'Unsupported HTTP method: {method}')

                response.raise_for_status()
                self.result = response.json()
                
        except httpx.HTTPStatusError as e:
            raise PluggToAPIError(f'HTTP error occurred: {e}')
        except httpx.RequestError as e:
            raise PluggToAPIError(f'Failed to connect: {e}')
        except ValueError as e:
            raise PluggToAPIError(str(e))
        except Exception as e:
            raise PluggToAPIError(f'Failed to connect: {e}')

    def update_price(self, sku: str, new_price: float):
        try:
            payload = [{'special_price': new_price},]
            payload_str = json.dumps(payload)                     
            self.connect(
                method='PUT',
                endpoint=f'/skus/{sku}',
                payload=payload_str,
            )
            plugg_to_api_logger.info(
                f'Product: {sku} updated price to {new_price}'
            )
        except PluggToAPIError as e:
            raise PluggToAPIError(f'Failed to update price: {e}')
        
    def update_prices(self, pricing_df: pd.DataFrame):
        try:            
            for index, row in pricing_df.iterrows():                
                self.update_price(
                    sku=str(row['sku (*)']), new_price=row['special_price']
                )
        except Exception as e:
            PluggToAPIError(e)
            raise
