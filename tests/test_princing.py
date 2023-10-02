import unittest

import pandas as pd

from kami_princing.princing import Pricing


class TestPricing(unittest.TestCase):
    def setUp(self):
        self.pc = Pricing(
            multiplier_commission=0.15,
            multiplier_admin=0.05,
            multiplier_reverse=0.003,
            limit_rate_ebitda=4.0,
            increment_price_new=0.10,
        )

    def test_calc_ebitda_positive(self):
        data = {
            'special_price': [10.0],
            'COST': [2.0],
            'FREIGHT': [1.0],
            'INPUT': [1.0],
        }
        df = pd.DataFrame(data)
        result_df = self.pc.calc_ebitda(df)
        self.assertEqual(result_df['EBITDA R$'][0], 3.97)
        self.assertEqual(result_df['EBITDA %'][0], 39.7)

    def test_pricing_positive(self):
        data = {
            'special_price': [10.0],
            'COST': [2.0],
            'FREIGHT': [1.0],
            'INPUT': [1.0],
        }
        df = pd.DataFrame(data)
        result_df = self.pc.pricing(df)
        self.assertGreaterEqual(result_df['EBITDA %'][0], 4.0)

    def test_pricing_negative(self):
        data = {
            'special_price': [0.0],
            'COST': [2.0],
            'FREIGHT': [1.0],
            'INPUT': [1.0],
        }
        df = pd.DataFrame(data)
        with self.assertRaises(ZeroDivisionError):
            self.pc.pricing(df)


if __name__ == '__main__':
    unittest.main()
