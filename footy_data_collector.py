import requests

import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FootyFlow - Data Collector")


class FootyCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://apiv3.apifootball.com/"
        self.timeout = 30
        self.connect_timeout = 5
        self.leagues_dict = dict()

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
        
    def get_leagues(self, country_id):
        url = f"{self.base_url}?action=get_leagues&country_id={country_id}&APIkey={self.api_key}"
        logger.info(f"Requesting leagues data from {url}")

        try:
            response = requests.get(url, timeout=(self.connect_timeout, self.timeout))
            response.raise_for_status()
            leagues = response.json()
            logger.info("Leagues data received successfully.")
            count = 0
            for league in leagues:
                count += 1
                season = league['league_season']
                league_name = league['league_name']
                league_id = league['league_id']
                self.leagues_dict[league_id] = {"league_name": league_name, "season": season}
            logger.info(f"Found {count} leagues")
            return self.leagues_dict
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while requesting country data: {e}")
            return None