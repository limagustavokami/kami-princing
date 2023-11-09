import time
import schedule
import json

from kami_pricing.constant import PRICING_MANAGER_FILE
from kami_pricing.pricing_manager import PricingManager, PricingManagerError, pricing_logger


def update_prices():
    pricing_manager = PricingManager.from_json(file_path=PRICING_MANAGER_FILE)
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