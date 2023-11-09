import time
import schedule
import json

from kami_pricing.constant import PRICING_MANAGER_FILE
from kami_pricing.pricing_manager import PricingManager, PricingManagerError


def create_pricing_manager_instance_from_json(json_file_path: str) -> PricingManager:
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    company = json_data.get('company')
    marketplace = json_data.get('marketplace')
    integrator = json_data.get('integrator')

    if not all([company, marketplace, integrator]):
        raise PricingManagerError("JSON file must contain 'company', 'marketplace', and 'integrator' keys.")

    pricing_manager_instance = PricingManager(company=company, marketplace=marketplace, integrator=integrator)

    return pricing_manager_instance


def update_prices():
    pricing_manager = create_pricing_manager_instance_from_json(json_file_path=PRICING_MANAGER_FILE)
    pricing_df = pricing_manager.scraping_and_pricing()
    pricing_manager.update_prices(pricing_df=pricing_df)

def main():
    schedule.every(10).minutes.do(update_prices)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()