from hvac_vault import HashiCorpAPIClient
from footy_data_collector import FootyCollector
from footy_data_loader import FootyLoader
import os
import logging

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
    
    def get_table_schema(self, data_sample):
        table_schema = {}
        for data in data_sample.values():
            table_schema['league_id'] = 'INTEGER PRIMARY KEY'
            for key, value in data.items():
                data_type = 'INTEGER' if isinstance(value, int) else 'TEXT'
                table_schema[key] = data_type
        return table_schema

    def insert_league_data(self, leagues):
        table_schema = self.get_table_schema(leagues)
        self.db.create_table_if_not_exists("leagues", table_schema)

        for league in leagues:
            values = {
                "league_id": int(league["league_id"]),
                "league_name": league["league_name"],
                "season": league["season"]
            }
            self.db.upsert_values("leagues", values, unique_columns=["league_id"])

    def close(self):
        self.db.close_connection()

if __name__ == "__main__":
    db_path = "/temp/footy.sqlite3"
    processor = FootyDataProcessor(db_path)

    country_set = 'Brazil'
    leagues = processor.process_country_data(country_set)
    processor.insert_league_data(leagues)

    processor.close()
