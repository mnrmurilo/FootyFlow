import sqlite3
import logging
from typing import Dict, List, Union

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FootyFlow - Data Loader")

class FootyLoader:
    def __init__(self, db_path: str):
        try:
            self.connection = sqlite3.connect(db_path)
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to the SQLite database at {db_path}.")
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to the SQLite database: {e}")

    def create_table_if_not_exists(self, table_name: str, schema: str):
        try:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
            self.cursor.execute(create_table_query)
            self.connection.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
        except sqlite3.Error as e:
            logger.error(f"Failed to create table '{table_name}': {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            logger.info("Connection to the SQLite database closed.")
