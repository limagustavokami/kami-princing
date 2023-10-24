import pandas as pd
import requests
from kami_gsuite.kami_gsheet import KamiGsheet


def get_urls_from_gsheet(sheet_id, sheet_range):
    try:

        gsheet = KamiGsheet(
            api_version='v4',
            credentials_path='../credentials/k_service_account_credentials.json',
        )

        urls = gsheet.convert_range_to_dataframe(
            sheet_id=sheet_id,
            sheet_range=sheet_range,
        )

        urls = list(urls['urls'])
        return urls

    except requests.RequestException as e:
        print(e)


teste = get_urls_from_gsheet(
    '1u7dCTQzbqgKSSjpSVtsUl7ea2j2YgW4Ko2nB9akE1ws', 'pricing!A1:A'
)

print(type(teste))
