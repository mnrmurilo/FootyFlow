import requests

from hvac_vault import HashiCorpAPIClient
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FootyFlow - Data Extraction")


class Extraction:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://apiv3.apifootball.com/"
        self.timeout = 30
        self.connect_timeout = 5

    def get_country_id(self, country_name):
        url = f"{self.base_url}?action=get_countries&APIkey={self.api_key}"
        logger.info(f"Requesting country data from {url}")

        try:
            response = requests.get(url, timeout=(self.connect_timeout, self.timeout))
            response.raise_for_status()
            countries = response.json()
            logger.info("Country data received successfully.")
            
            for country in countries:
                if country['country_name'] == country_name:
                    country_id = country['country_id']
                    logger.info(f"Found country '{country_name}' with ID: {country_id}")
                    return country_id
            
            logger.warning(f"Country '{country_name}' not found in the response.")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while requesting country data: {e}")
            return None

# Example usage
if __name__ == "__main__":
    client = HashiCorpAPIClient()
    football_api = Extraction(client.get_secret())

    country_set = 'Germany'
    country_id = football_api.get_country_id(country_set)
    
    if country_id:
        logger.info(f"Country ID for {country_set}: {country_id}")
    else:
        logger.warning(f"Country '{country_set}' not found.")