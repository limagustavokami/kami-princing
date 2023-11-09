import time
import schedule
import json

from kami_pricing.constant import PRICING_MANAGER_FILE
from kami_pricing.pricing_manager import PricingManager, PricingManagerError, pricing_logger


def create_pricing_manager_instance_from_json(json_file_path: str) -> PricingManager:
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    company = json_data.get('company')
    marketplace = json_data.get('marketplace')
    products_urls_sheet_name = json_data.get('products_urls_sheet_name')
    skus_sellers_sheet_name = json_data.get('skus_sellers_sheet_name')
    integrator = json_data.get('integrator')    

    if not all([company, marketplace, integrator]):
        raise PricingManagerError("JSON file must contain 'company', 'marketplace', 'products_urls_sheet_name', 'skus_sellers_sheet_name' and 'integrator' keys.")

    pricing_manager_instance = PricingManager(
        company=company,        
        marketplace=marketplace,
        products_ulrs_sheet_name=products_urls_sheet_name,
        skus_sellers_sheet_name=skus_sellers_sheet_name,
        integrator=integrator
    )

    return pricing_manager_instance

def update_prices():
    pricing_manager = create_pricing_manager_instance_from_json(json_file_path=PRICING_MANAGER_FILE)
    pricing_df = pricing_manager.scraping_and_pricing()
    pricing_manager.update_prices(pricing_df=pricing_df)

def main():
    with open(PRICING_MANAGER_FILE, 'r') as file:
        json_data = json.load(file)
        secs = json_data.get('every_seconds')
    schedule.every(secs).seconds.do(update_prices)
    while True:
        try:
            schedule.run_pending()        
            time.sleep(1)
        except Exception as e:
            pricing_logger.info(f'Error when updating prices: {e}')
            

if __name__ == '__main__':
    main()