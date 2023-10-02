import requests
import json
from typing import Union

class PluggToAPI:
    def __init__(self, credentials_path: str, base_url: str = "https://api.plugg.to"):
        self.base_url = base_url
        self.credentials_path = credentials_path
        self.access_token = None
        self._set_access_token()
        
    def connect(self) -> str:
        try:
            with open(self.credentials_path, 'r') as file:
                credentials = json.load(file)
            payload = {
                "client_id": credentials['client_id'],
                "client_secret": credentials['client_secret'],
                "username": credentials['username'],
                "password": credentials['password'],
                "grant_type": "password"
            }
            url = f"{self.base_url}/oauth/token"
            headers = {
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
            return response.json()['access_token']
        except Exception as e:
            raise Exception(f"Failed to connect: {e}")

    def _set_access_token(self):
        try:
            self.access_token = self.connect()
        except Exception as e:
            raise Exception(f"Failed to set access token: {e}")

    def update_special_price(self, sku: str, price: Union[float, int]) -> None:
        try:
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            url = f"{self.base_url}/skus/{sku}"
            data = {
                "special_price": price
            }
            response = requests.put(url, headers=headers, json=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to update special price: {e}")
        except Exception as e:
            raise Exception(f"An error occurred: {e}")