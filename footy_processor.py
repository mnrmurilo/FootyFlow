from hvac_vault import HashiCorpAPIClient
from footy_data_collector import FootyCollector
from footy_data_loader import FootyLoader
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FootyFlow - Processor")

class FootyDataProcessor:
    def __init__(self, db_path):
        self.current_path = os.getcwd()
        os.makedirs(self.current_path + '/temp', exist_ok=True)
        self.db = FootyLoader(self.current_path + db_path)

    def process_country_data(self, country_name):
        client = HashiCorpAPIClient()
        footy_collector = FootyCollector(client.get_secret())
        
        country_id = footy_collector.get_country_id(country_name)
        leagues = footy_collector.get_leagues(country_id)
        return leagues

    def insert_league_data(self, leagues):
        table_schema = self.db.get_table_schema(leagues)
        self.db.create_table_if_not_exists("leagues", table_schema)
        for league in leagues.keys():
            values = {
                "league_id": int(league),
                "league_name": leagues[league]['league_name'],
                "season": leagues[league]["season"]
            }
            self.db.upsert_values("leagues", values, unique_columns=["league_id"])

    def close(self):
        self.db.close_connection()

class MainProcessor:
    def __init__(self):
        self.processor = FootyDataProcessor(os.getenv("DB_PATH"))

    def run(self):
        country_set = 'Brazil'
        leagues = self.processor.process_country_data(country_set)
        self.processor.insert_league_data(leagues)
        self.processor.close()

if __name__ == "__main__":
    main = MainProcessor()
    main.run()
