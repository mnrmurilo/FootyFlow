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
    
    def get_table_schema(self, data_sample):
        table_schema = {}
        for data in data_sample.values():
            table_schema['league_id'] = 'INTEGER PRIMARY KEY'
            for key, value in data.items():
                data_type = 'INTEGER' if isinstance(value, int) else 'TEXT'
                table_schema[key] = data_type
        return table_schema

    def create_table_if_not_exists(self, table_name, schema):
        try:
            columns = ', '.join([f"{column} {datatype}" for column, datatype in schema.items()])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            self.cursor.execute(create_table_query)
            self.connection.commit()
            logger.info(f"Table '{table_name}' checked/created successfully.")
        except sqlite3.Error as e:
            logger.error(f"Failed to create table '{table_name}': {e}")

    def upsert_values(self, table_name: str, values: Dict[str, Union[str, int]], unique_columns: List[str]):
        try:
            columns = ', '.join(values.keys())
            placeholders = ', '.join(['?'] * len(values))
            update_placeholders = ', '.join([f"{col} = excluded.{col}" for col in values.keys() if col not in unique_columns])

            unique_constraint = ', '.join(unique_columns)
            query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({unique_constraint})
                DO UPDATE SET {update_placeholders};
            """
            logger.debug(f"Executing query: {query} with values: {list(values.values())}")
            self.cursor.execute(query, tuple(values.values()))
            self.connection.commit()
            logger.info(f"Upsert operation completed for table '{table_name}'.")
        except sqlite3.Error as e:
            logger.error(f"Failed to upsert values into table '{table_name}': {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            logger.info("Connection to the SQLite database closed.")
