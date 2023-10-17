import http.client
import json
import pandas as pd



class Anymarket:
    def __init__(
        self, credentials_path: str, base_url: str = "/v2/products"
    ):
        self.credentials_path = credentials_path
        #self.base_url = base_url

    def create_anymarket_products(self, df:pd.DataFrame):
        products_list = []

        for product in df['content']:
            products_list.append(product['skus'][0])
        
        df = pd.DataFrame(products_list)

        df.rename(columns={'partnerId':'sku (*)'}, inplace=True)

        df = df[['sku (*)','id','title','ean','price','amount','additionalTime','stockLocalId']]

        return df

    def connect(self, base_url) -> json:
        try:
            with open(self.credentials_path, 'r') as file:
                credentials = json.load(file)
                conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

                headers = {
                    'Content-Type': "application/json",
                    'gumgaToken': credentials['anymarket_token'],
                    }

                conn.request("GET", base_url, headers=headers)

                return conn.getresponse()
        
        except Exception as e:
            raise Exception(f'Failed to connect: {e}')
        
    def create_anymarket_products(self, df:pd.DataFrame):
        products_list = []

        print(df)

        data_list = []

        for product in df["content"]:
            product_data = {
                "id": product["id"],
                "title": product["title"],
                "description": product["description"],
                "category_id": product["category"]["id"],
                "category_name": product["category"]["name"],
                "category_path": product["category"]["path"],
                "origin_id": product["origin"]["id"],
                "calculatedPrice": product["calculatedPrice"],
                "definitionPriceScope": product["definitionPriceScope"],
                "hasVariations": product["hasVariations"],
                "isProductActive": product["isProductActive"],
            }

            data_list.append(product_data)

        df = pd.DataFrame(data_list)

        return df

    def get_all_products(self) -> pd.DataFrame:
        try:
            conector = self.connect('/v2/products')
            data = conector.read()
            json_data = json.loads(data.decode("utf-8"))

            df = self.create_anymarket_products(json_data)

            return df
        except Exception as e:
            raise Exception(f'Failed to connect: {e}')

    # def change_calculated_price_to_false(self, df:pd.DataFrame):
    #     try:
    #         conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

    #         for id in df['id']:
    #             payload = "{\n  \"title\": \"replace\",\n  \"description\": \"Título do produto\",\n  \"maxLength\": 150\n}"

    #             conn.request("PATCH", "/v2/products/id", payload, headers)

    #             conector = self.connect(f'/v2/products/{id}/changeCalculatedPrice')
    #             data = conector.read()
    #             json_data = json.loads(data.decode("utf-8"))

        
    #     except Exception as e:
    #         raise Exception(f'Failed to connect: {e}')





if __name__ == "__main__":
    am = Anymarket('../credentials/k_service_account_credentials.json')
    all_products = am.get_all_products()

    print(all_products)

    

