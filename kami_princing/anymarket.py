import http.client
import json
import pandas as pd



class Anymarket:
    def __init__(
        self, credentials_path: str, base_url: str = "/v2/products"
    ):
        self.credentials_path = credentials_path

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

    def get_products_quantity(self) -> int:
        try:
            conector = self.connect('/v2/products')
            data = conector.read()
            json_data = json.loads(data.decode("utf-8"))

            # Acesse o valor de 'totalElements' e guarde em uma variável
            total_elements = json_data['page']['totalElements']

            return total_elements
        except Exception as e:
            raise Exception(f'Failed to connect: {e}')

    def get_all_products(self, quantity:int) -> pd.DataFrame:
        try:
            conector = self.connect(f'/v2/products?limit={quantity}')
            data = conector.read()
            json_data = json.loads(data.decode("utf-8"))

            products = json_data.get('content', [])
            partner_ids = [sku.get('partnerId', '') for product in products for sku in product.get('skus', [])]

            product_ids = [product['id'] for product in json_data.get('content', [])]
            print(json_data)
            return partner_ids, product_ids
        except Exception as e:
            raise Exception(f'Failed to connect: {e}')
        
    def change_data_patch(self, list: list):
        try:
            for id in list:


                conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

                payload = "{\n  \"calculatedPrice\": false,\n  \"definitionPriceScope\": \"SKU_MARKETPLACE\"\n}"

                headers = {
                    'Content-Type': "application/merge-patch+json",
                    'gumgaToken': "32293584L39959642E1789758617689C169644661768900O891.I"
                }

                conn.request("PATCH", f"/v2/products/{id}", payload, headers)

                res = conn.getresponse()
                data = res.read()

                print(data.decode("utf-8"))

        except Exception as e:
            print(e)

    def query_all_products(self, id_list: list):
        try:
            result_list = []

            for product_id in id_list:
                conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

                headers = {
                    'Content-Type': "application/json",
                    'gumgaToken': "32293584L39959642E1789758617689C169644661768900O891.I"
                }

                conn.request("GET", f"/v2/skus/marketplaces?partnerID={product_id}", headers=headers)

                res = conn.getresponse()
                data = res.read()
                json_data = json.loads(data.decode("utf-8"))
                result_list.extend(json_data)

            # Criar um DataFrame a partir de uma lista de dicionários
            df = pd.json_normalize(result_list)
            
            # Renomear a coluna 'skuInMarketplace' para 'sku (*)'
            df.rename(columns={'skuInMarketplace': 'sku (*)'}, inplace=True)

            # Selecionar as colunas desejadas
            df = df[['sku (*)', 'id', 'marketPlace', 'publicationStatus', 'marketplaceStatus', 'price', 'fields.title']]

            # Remover coluna vazia
            df.dropna(subset=['fields.title'], inplace=True)

            return df

        except Exception as e:
            print(e)

    def change_price(self, marketplace: str, df: pd.DataFrame):
        try:
            conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

            headers = {
                'Content-Type': "application/json",
                'gumgaToken': "32293584L39959642E1789758617689C169644661768900O891.I"
            }

            for index, row in df[df['marketPlace'] == marketplace].iterrows():
                

                payload = [
                    {
                        "id": row['id'],
                        "price": row['special_price'],
                        "discountPrice": row['special_price']
                    }
                ]

                payload_str = json.dumps(payload)

                conn.request("PUT", "/v2/skus/marketplaces/prices", payload_str, headers)

                res = conn.getresponse()
                data = res.read()

                print(data.decode("utf-8"))

        except Exception as e:
            print(e)
    
    def get_from_marketplace(self, marketplace: str):
        conn = http.client.HTTPConnection("sandbox-api.anymarket.com.br")

        headers = {
            'Content-Type': "application/json",
            'gumgaToken': "32293584L39959642E1789758617689C169644661768900O891.I"
            }

        conn.request("GET", "/v2/transmissions/marketplace/0/0/sort/statusFilter", headers=headers)

        res = conn.getresponse()
        data = res.read()

        print(data.decode("utf-8"))


if __name__ == "__main__":
    am = Anymarket('../credentials/k_service_account_credentials.json')

    quantity = am.get_products_quantity()

    all_products = am.get_all_products(quantity)

    change = am.change_data_patch(all_products[1])

    query = am.query_all_products(all_products[0])

    print(query)

    change = am.change_price('ECOMMERCE', query)



    





    

