import http.client
import json
import pandas as pd



class Anymarket:
    def __init__(
        self, credentials_path: str, base_url: str = "/v2/products"
    ):
        self.credentials_path = credentials_path
        self.base_url = base_url

    def create_anymarket_products(self, df:pd.DataFrame):
        products_list = []

        for product in df['content']:
            products_list.append(product['skus'][0])
        
        df = pd.DataFrame(products_list)

        df.rename(columns={'partnerId':'sku (*)'}, inplace=True)

        df = df[['sku (*)','id','title','ean','price','amount','additionalTime','stockLocalId']]

        return df

    def connect(self) -> json:
        try:
            with open(self.credentials_path, 'r') as file:
                credentials = json.load(file)
                conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

                headers = {
                    'Content-Type': "application/json",
                    'gumgaToken': credentials['anymarket_token'],
                    }

                conn.request("GET", self.base_url, headers=headers)

                res = conn.getresponse()
                data = res.read()
                json_data = json.loads(data.decode("utf-8"))

                df = self.create_anymarket_products(json_data)

                return df
        except Exception as e:
            raise Exception(f'Failed to connect: {e}')

    def create_anymarket_products(self, df:pd.DataFrame):
        products_list = []

        for product in df['content']:
            products_list.append(product['skus'][0])
        
        df = pd.DataFrame(products_list)

        df.rename(columns={'partnerId':'sku (*)'}, inplace=True)

        df = df[['sku (*)','id','title','ean','price','amount','additionalTime','stockLocalId']]

        return df


if __name__ == "__main__":
    am = Anymarket('../credentials/k_service_account_credentials.json')
    teste = am.connect()


    print(teste)

