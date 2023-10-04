import unittest

import pandas as pd

from kami_princing.princing import Pricing


class TestPricing(unittest.TestCase):

    pc = Pricing()

    def setUp(self):
        self.pc = Pricing(
            multiplier_commission=0.15,
            multiplier_admin=0.05,
            multiplier_reverse=0.003,
            limit_rate_ebitda=4.0,
            increment_price_new=0.10,
        )

    def test_calc_ebitda_success(self):

        data = {
            'sku': 'BR001',
            'special_price': [02.0],
            'COST': [2.0],
            'FREIGHT': [1.0],
            'INPUT': [1.0],
        }
        df = pd.DataFrame(data)

        ebitda = self.pc.calc_ebitda(df)
        df_ebitda = self.pc.pricing(ebitda)
        self.assertEqual(df_ebitda['EBITDA %'][0], 4.0)

    def test_calc_ebitda_failure(self):

        data = {
            'sku': 'BR001',
            'special_price': [02.0],
            'COST': [2.0],
            'FREIGHT': [1.0],
            'INPUT': [1.0],
        }
        df = pd.DataFrame(data)

        ebitda = self.pc.calc_ebitda(df)
        df_ebitda = self.pc.pricing(ebitda)
        self.assertNotEqual(df_ebitda['EBITDA %'][0], 3.9)


if __name__ == '__main__':
    unittest.main()
