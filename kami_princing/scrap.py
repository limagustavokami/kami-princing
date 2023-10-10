import logging
from datetime import datetime
from typing import List
import pandas as pd
import requests
from bs4 import BeautifulSoup
from kami_logging import benchmark_with, logging_with
from kami_gsuite.kami_gsheet import KamiGsheet
import json


from constant import (
    COLUMNS_ALL_SELLER,
    COLUMNS_DIFERENCE,
    COLUMNS_EXCEPT_HAIRPRO,
    COLUMNS_HAIRPRO,
    COLUMNS_RESULT,
)

princing_logger = logging.getLogger('Pricing')

def get_urls_from_gsheet(sheet_id):
    try:
        gsheet = KamiGsheet(api_version='v4', 
                            credentials_path='../credentials/k_service_account_credentials.json')
        
        urls = gsheet.convert_range_to_dataframe(
            sheet_id=sheet_id,
            sheet_range="pricing!A1:A",
        )

        skus = gsheet.convert_range_to_dataframe(
            sheet_id=sheet_id,
            sheet_range="skushairpro!A1:B",
        )

        urls = list(urls['urls'])
        return urls, skus
    
    except requests.RequestException as e:
            print(e)

class Scraper:

    def __init__(self, marketplace: str):
        self.marketplace = marketplace

    def fetch_page_content(self,urls:List[str]):   
        try:
            sellers_df_list = []

            for url in urls:
                response = requests.get(
                    url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0'
                    },
                )
                
                soup = BeautifulSoup(response.content, 'html.parser') 
                id_sellers = soup.find_all('a', class_='btn btn-block btn-primary btn-lg js-add-to-cart')    

                for id_seller in id_sellers:           
                    sellers = id_seller.get('data-sku')            
                    row = json.loads(sellers)[0]
                    "\n"          
                    
                    print(
                        f"Extraindo dados do vendedor Id: {row['seller']['id']} \
                            | Loja: {row['seller']['name']} ")


                    sellers_row = [row['sku'],  row['brand'],  row['category'], row['name'],
                                row['price'], row['seller']['name']]
                    sellers_df_list.append(sellers_row)
            
            return sellers_df_list

        except requests.RequestException as e:
            print(e)
        
    

if __name__ == "__main__":
    sheets = get_urls_from_gsheet("1u7dCTQzbqgKSSjpSVtsUl7ea2j2YgW4Ko2nB9akE1ws")
    print(sheets[0])


