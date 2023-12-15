import logging
from datetime import datetime
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from kami_logging import benchmark_with, logging_with

from kami_princing.constant import (
    COLUMNS_ALL_SELLER,
    COLUMNS_DIFERENCE,
    COLUMNS_EXCEPT_HAIRPRO,
    COLUMNS_HAIRPRO,
    COLUMNS_RESULT,
)

princing_logger = logging.getLogger('Pricing')


class Scraper:
    def __init__(self, urls: List[str], sku_sellers_df: pd.DataFrame):
        self.urls = urls
        self.sku_sellers_df = sku_sellers_df
        self.sellers_df_list = []
        self.all_sellers_df = None
        self.hairpro_df = None
        self.except_hairpro_df = None
        self.difference_price_df = None
        self.pricing_result = None
        self.df_pricing = None

    def fetch_page_content(self, url):
        try:
            response = requests.get(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0'
                },
            )
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            princing_logger.error(f'Failed to retrieve URL {url}: {e}')
            raise

    def extract_seller_data(self, soup):
        id_sellers = soup.find_all(
            'a', class_='btn btn-block btn-primary btn-lg js-add-to-cart'
        )
        for id_seller in id_sellers:
            sellers = id_seller.get('data-sku')
            row = pd.json_normalize(sellers)[0]
            sellers_row = [
                row['sku'],
                row['brand'],
                row['category'],
                row['name'],
                row['price'],
                row['seller']['name'],
            ]
            self.sellers_df_list.append(sellers_row)

    def gather_seller_data(self):
        for url in self.urls:
            soup = self.fetch_page_content(url)
            self.extract_seller_data(soup)

    def prepare_all_sellers_df(self):
        self.all_sellers_df = pd.DataFrame(
            self.sellers_df_list, columns=COLUMNS_ALL_SELLER
        )
        self.all_sellers_df.drop('category', axis=1, inplace=True)
        data_string = datetime.now().strftime('%Y-%m-%d')
        self.all_sellers_df['data'] = data_string

    def filter_seller_data(self):
        seller_name = self.all_sellers_df.groupby('seller_name')
        self.hairpro_df = seller_name.get_group('HAIRPRO')[
            COLUMNS_HAIRPRO
        ].copy()
        self.except_hairpro_df = self.all_sellers_df[
            self.all_sellers_df['seller_name'].str.contains('HAIRPRO')
        ][COLUMNS_EXCEPT_HAIRPRO].copy()
        
        sugest_price = self.except_hairpro_df.groupby('sku')['price'].idxmin()

        self.except_hairpro_df = self.except_hairpro_df.loc[sugest_price]


    def calculate_price_differences(self):
        self.difference_price_df = self.hairpro_df[COLUMNS_DIFERENCE].copy()
        merged_df = self.hairpro_df.merge(
            self.except_hairpro_df, on='sku', suffixes=('', '_competitor')
        )
        self.difference_price_df['competitor_price'] = merged_df[
            'price_competitor'
        ]
        self.difference_price_df['difference_price'] = (
            merged_df['price_competitor'] - merged_df['price'] - 0.10
        ).round(6)
        self.difference_price_df[
            'suggest_price'
        ] = self.difference_price_df.apply(
            self.calculate_suggested_price, axis=1
        )
        self.difference_price_df['gain_%'] = (
            (
                self.difference_price_df['suggest_price']
                / self.difference_price_df['price']
            )
            - 1
        ).round(2) * 100

    @staticmethod
    def calculate_suggested_price(row):
        if pd.isna(row['competitor_price']):
            return row['price'].round(6)
        elif row['price'].min() < row['competitor_price'].max():
            return row['competitor_price'].round(6) - 0.10

    def merge_and_prepare_final_df(self):
        self.pricing_result = self.difference_price_df.merge(
            self.sku_sellers_df, how='left'
        )
        self.df_pricing = (
            self.pricing_result[
                [
                    'sku_kami',
                    'suggest_price',
                    'competitor_price',
                    'difference_price',
                ]
            ]
            .copy()
            .dropna()
        )
        self.df_pricing.rename(
            columns={'suggest_price': 'special_price', 'sku_kami': 'sku (*)'},
            inplace=True,
        )

    def get_final_dataframe(self):
        try:
            self.gather_seller_data()
            self.prepare_all_sellers_df()
            #self.remove_duplicates_and_unwanted_rows()
            self.filter_seller_data()
            self.calculate_price_differences()
            self.merge_and_prepare_final_df()
            return self.df_pricing
        except Exception as e:
            princing_logger.error(f'Failed to compute final dataframe: {e}')
            raise
