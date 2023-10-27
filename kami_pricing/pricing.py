import logging

import pandas as pd
from kami_logging import benchmark_with, logging_with

dataframe_logger = logging.getLogger('dataframe')


class Pricing:
    def __init__(
        self,
        multiplier_commission: float = 0.0,
        multiplier_admin: float = 0.0,
        multiplier_reverse: float = 0.0,
        limit_rate_ebitda: float = 0.0,
        increment_price_new: float = 0.0,
    ):
        self.multiplier_commission = multiplier_commission
        self.multiplier_admin = multiplier_admin
        self.multiplier_reverse = multiplier_reverse
        self.limit_rate_ebitda = limit_rate_ebitda
        self.increment_price_new = increment_price_new

    @benchmark_with(dataframe_logger)
    @logging_with(dataframe_logger)
    def calc_ebitda(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df['COMMISSION'] = round(
                df['special_price'] * self.multiplier_commission, 2
            )
            df['ADMIN'] = round(df['special_price'] * self.multiplier_admin, 2)
            df['REVERSE'] = round(
                df['special_price'] * self.multiplier_reverse, 2
            )
            df['EBITDA R$'] = (
                df['special_price']
                - df['COST']
                - df['FREIGHT']
                - df['INPUT']
                - df['COMMISSION']
                - df['ADMIN']
                - df['REVERSE']
            )
            df['EBITDA %'] = (
                round(df['EBITDA R$'] / df['special_price'], 3) * 100
            )
            df = df.dropna(subset=['EBITDA R$'], axis=0, how='any')
            df = df.reset_index()
            return df
        except ZeroDivisionError:
            dataframe_logger.error(
                'Division by zero encountered while calculating percentages.'
            )
        except Exception as e:
            dataframe_logger.error(f'An unexpected error occurred: {e}')
            return None

    @benchmark_with(dataframe_logger)
    @logging_with(dataframe_logger)
    def pricing(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.calc_ebitda(df)
        if df is None:
            return None

        try:
            for idx in range(0, len(df['special_price'])):
                while df.loc[idx, 'EBITDA %'] < self.limit_rate_ebitda:
                    df.loc[idx, 'special_price'] += self.increment_price_new
                    df.loc[idx, 'COMMISSION'] = round(
                        df.loc[idx, 'special_price']
                        * self.multiplier_commission,
                        2,
                    )
                    df.loc[idx, 'ADMIN'] = round(
                        df.loc[idx, 'special_price'] * self.multiplier_admin,
                        2,
                    )
                    df.loc[idx, 'REVERSE'] = round(
                        df.loc[idx, 'special_price'] * self.multiplier_reverse,
                        2,
                    )
                    df.loc[idx, 'EBITDA R$'] = (
                        df.loc[idx, 'special_price']
                        - df.loc[idx, 'COST']
                        - df.loc[idx, 'COMMISSION']
                        - df.loc[idx, 'FREIGHT']
                        - df.loc[idx, 'ADMIN']
                        - df.loc[idx, 'INPUT']
                        - df.loc[idx, 'REVERSE']
                    )
                    df.loc[idx, 'EBITDA %'] = (
                        round(
                            df.loc[idx, 'EBITDA R$']
                            / df.loc[idx, 'special_price'],
                            2,
                        )
                        * 100
                    )
                else:
                    dataframe_logger.info(
                        f"The sku {df.loc[idx, 'sku']} with a price of {df.loc[idx, 'special_price']} has an ebitda of {df.loc[idx, 'EBITDA %']}"
                    )
            return df
        except Exception as e:
            dataframe_logger.error(f'An unexpected error occurred: {e}')
            return None
