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
        urls = list(urls['urls'])

        sku_sellers = gsheet.convert_range_to_dataframe(
            sheet_id=sheet_id,
            sheet_range="skushairpro!A1:B",
        )

        sku_sellers = sku_sellers.rename(columns={0: 'SKU Seller', 1: 'SKU Beleza'})

        return urls, sku_sellers
    
    except requests.RequestException as e:
            print(e)

class Scraper:

    def __init__(self, marketplace: str):
        self.marketplace = marketplace

    def scrap_from_pages(self,urls:List[str]):   
        try:
            sellers_df_list = []

            for url in urls[0]:
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
        
    def create_dataframes(self, df:pd.DataFrame, skus:pd.DataFrame):

        df_sellers_df_list = pd.DataFrame(df, columns=COLUMNS_ALL_SELLER)
        df_sellers_df_list.drop_duplicates(keep='first', inplace=True)
        df_sellers_df_list.drop(df_sellers_df_list[df_sellers_df_list['seller_name'].str.contains('Beleza na Web')].index, inplace=True)
        hairpro_df = df_sellers_df_list.loc[df_sellers_df_list['seller_name'] == 'HAIRPRO']
        except_hairpro_df = df_sellers_df_list.drop(df_sellers_df_list[df_sellers_df_list['seller_name'].str.contains('HAIRPRO')].index)
        except_hairpro_df = pd.DataFrame(except_hairpro_df,  columns=COLUMNS_EXCEPT_HAIRPRO)
        except_hairpro_df.drop_duplicates(subset='sku', keep='first', inplace=True)
        difference_price_df = pd.DataFrame(hairpro_df, columns=COLUMNS_DIFERENCE)

        for i in hairpro_df['sku']:
            for j in except_hairpro_df['sku']:
                if i == j:
                    difference_price_df.loc[difference_price_df['sku'] == i, 'competitor_price'] = except_hairpro_df.loc[except_hairpro_df['sku'] == j, 'price'].values[0]
                    difference_price_df['difference_price'] = difference_price_df['competitor_price'] - difference_price_df['price'] - 0.10
                    difference_price_df['difference_price'] = difference_price_df['difference_price'].round(6)
                    print(difference_price_df['difference_price'])
                # quando difference_price_df['competitor_price'] for zero e sua serie for ambigua, sugerir um preco de 10% maior    
                # e arrendondar para 2 casas decimais o preco sugerido
                if difference_price_df['competitor_price'].isnull().values.any():
                    difference_price_df['suggest_price'] = difference_price_df['price'].round(6)
                    print(difference_price_df['suggest_price'])

                # quando o preço da Hairpro for maior que o preço do concorrente, sugerir o preço de 0,10 centavos a menos
                # que o preço do concorrente e arrendondar para 2 casas decimais o preco sugerido            
                if difference_price_df['price'].min() < difference_price_df['competitor_price'].max():
                    difference_price_df['suggest_price'] = difference_price_df['competitor_price'].round(6) - 0.10

                # percentual de diferença entre o preço da Hairpro e o preço do concorrente
                difference_price_df['ganho_%'] = (difference_price_df['suggest_price'] / difference_price_df['price']) -1
                difference_price_df['ganho_%'] = difference_price_df['ganho_%'].round(2) * 100
    
        sku_sellers = skus.rename(columns={'SKU Seller': 'sku_kami','SKU Beleza': 'sku'})
        sku_sellers = sku_sellers[['sku','sku_kami']]
        pricing_result = difference_price_df.merge(sku_sellers, how='left')
        df_pricing = pricing_result[['sku_kami','suggest_price','competitor_price']]
        df_pricing = df_pricing.dropna()
        df_pricing = df_pricing.rename(columns={'suggest_price':'special_price','sku_kami':'sku (*)'})
        
        return df_pricing
    
if __name__ == "__main__":
    sc = Scraper(marketplace='belezanaweb')

    urls_sheets_skus = get_urls_from_gsheet("1u7dCTQzbqgKSSjpSVtsUl7ea2j2YgW4Ko2nB9akE1ws")

    sellers_df_list = sc.scrap_from_pages(urls_sheets_skus)

    df_sellers_df_list = sc.create_dataframes(sellers_df_list,urls_sheets_skus[1])
    
    df_sellers_df_list.to_excel('pricing.xlsx')
    print(df_sellers_df_list)



