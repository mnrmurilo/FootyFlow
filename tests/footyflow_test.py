import unittest
import os
import sys
import requests
from unittest.mock import patch, MagicMock

# Adding project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from hvac_vault import HashiCorpAPIClient
from footy_data_collector import FootyCollector
from footy_data_loader import FootyLoader
from footy_processor import FootyDataProcessor, MainProcessor



class TestHashiCorpAPIClient(unittest.TestCase):

    @patch.dict(os.environ, {'HCP_CLIENT_ID': 'test_client_id', 'HCP_CLIENT_SECRET': 'test_client_secret'})
    @patch('requests.post')
    def test_get_access_token(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'access_token': 'test_access_token'}

        client = HashiCorpAPIClient()
        client._get_access_token()

        self.assertEqual(client.access_token, 'test_access_token')
        mock_post.assert_called_once()

    @patch.dict(os.environ, {'HCP_CLIENT_ID': 'test_client_id', 'HCP_CLIENT_SECRET': 'test_client_secret'})
    @patch('requests.post')
    def test_get_access_token_http_error(self, mock_post):
        mock_post.return_value.status_code = 400
        mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError('test error')

        client = HashiCorpAPIClient()

        with self.assertRaises(requests.exceptions.HTTPError):
            client._get_access_token()

    @patch.dict(os.environ, {'HCP_CLIENT_ID': 'test_client_id', 'HCP_CLIENT_SECRET': 'test_client_secret'})
    @patch('requests.post')
    def test_get_secret(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'access_token': 'test_access_token'}
        mock_get = MagicMock()
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {'secrets': [{'version': {'value': 'test_secret'}}]}
        requests.get = mock_get

        client = HashiCorpAPIClient()
        secret = client.get_secret()

        self.assertEqual(secret, 'test_secret')
        mock_post.assert_called_once()
        mock_get.assert_called_once()

    @patch.dict(os.environ, {'HCP_CLIENT_ID': 'test_client_id', 'HCP_CLIENT_SECRET': 'test_client_secret'})
    @patch('requests.post')
    def test_get_secret_http_error(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'access_token': 'test_access_token'}
        mock_get = MagicMock()
        mock_get.return_value.status_code = 400
        mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError('test error')
        requests.get = mock_get

        client = HashiCorpAPIClient()

        with self.assertRaises(requests.exceptions.HTTPError):
            client.get_secret()

    @patch.dict(os.environ, {'HCP_CLIENT_ID': 'test_client_id', 'HCP_CLIENT_SECRET': 'test_client_secret'})
    @patch('requests.post')
    def test_get_secret_no_secrets(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'access_token': 'test_access_token'}
        mock_get = MagicMock()
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {'secrets': []}
        requests.get = mock_get

        client = HashiCorpAPIClient()
        secret = client.get_secret()

        self.assertIsNone(secret)
        mock_post.assert_called_once()
        mock_get.assert_called_once()

class TestFootyCollector(unittest.TestCase):
    @patch('requests.get')
    def test_get_country_id(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {'country_id': 1, 'country_name': 'Brazil'},
            {'country_id': 2, 'country_name': 'Argentina'}
        ]
        collector = FootyCollector('test_api_key')
        country_id = collector.get_country_id('Brazil')
        self.assertEqual(country_id, 1)

    @patch('requests.get')
    def test_get_country_id_not_found(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {'country_id': 1, 'country_name': 'Brazil'},
            {'country_id': 2, 'country_name': 'Argentina'}
        ]
        collector = FootyCollector('test_api_key')
        country_id = collector.get_country_id('Germany')
        self.assertIsNone(country_id)

    @patch('requests.get')
    def test_get_leagues(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {'league_id': 1, 'league_name': 'Serie A', 'league_season': '2023'},
            {'league_id': 2, 'league_name': 'Serie B', 'league_season': '2023'}
        ]
        collector = FootyCollector('test_api_key')
        leagues = collector.get_leagues(1)
        self.assertEqual(leagues, {1: {'league_name': 'Serie A', 'season': '2023'}, 2: {'league_name': 'Serie B', 'season': '2023'}})

class TestFootyLoader(unittest.TestCase):

    def setUp(self):
        self.db_path = ':memory:'
        self.loader = FootyLoader(self.db_path)

    def test_get_table_schema(self):
        sample_data = {
            1: {'league_name': 'Serie A', 'season': '2023'},
            2: {'league_name': 'Serie B', 'season': '2023'}
        }
        schema = self.loader.get_table_schema(sample_data)
        self.assertEqual(schema, {
            'league_id': 'INTEGER PRIMARY KEY',
            'league_name': 'TEXT',
            'season': 'TEXT'
        })

    def test_create_table_if_not_exists(self):
        schema = {'league_id': 'INTEGER PRIMARY KEY', 'league_name': 'TEXT', 'season': 'TEXT'}
        self.loader.create_table_if_not_exists('leagues', schema)
        self.loader.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='leagues'")
        table_exists = self.loader.cursor.fetchone()
        self.assertIsNotNone(table_exists)

    def test_upsert_values(self):
        schema = {'league_id': 'INTEGER PRIMARY KEY', 'league_name': 'TEXT', 'season': 'TEXT'}
        self.loader.create_table_if_not_exists('leagues', schema)
        values = {'league_id': 1, 'league_name': 'Serie A', 'season': '2023'}
        self.loader.upsert_values('leagues', values, unique_columns=['league_id'])
        self.loader.cursor.execute("SELECT * FROM leagues WHERE league_id = 1")
        result = self.loader.cursor.fetchone()
        self.assertEqual(result, (1, 'Serie A', '2023'))

        # Test update
        values = {'league_id': 1, 'league_name': 'Serie A Updated', 'season': '2023'}
        self.loader.upsert_values('leagues', values, unique_columns=['league_id'])
        self.loader.cursor.execute("SELECT * FROM leagues WHERE league_id = 1")
        result = self.loader.cursor.fetchone()
        self.assertEqual(result, (1, 'Serie A Updated', '2023'))

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None

class TestFootyDataProcessor(unittest.TestCase):

    @patch('hvac_vault.HashiCorpAPIClient.get_secret')
    @patch('footy_data_collector.FootyCollector.get_country_id')
    @patch('footy_data_collector.FootyCollector.get_leagues')
    def test_process_country_data(self, mock_get_leagues, mock_get_country_id, mock_get_secret):
        mock_get_secret.return_value = 'test_api_key'
        mock_get_country_id.return_value = 1
        mock_get_leagues.return_value = {
            1: {'league_name': 'Serie A', 'season': '2023'},
            2: {'league_name': 'Serie B', 'season': '2023'}
        }
        processor = FootyDataProcessor(':memory:')
        leagues = processor.process_country_data('Brazil')
        self.assertEqual(leagues, {
            1: {'league_name': 'Serie A', 'season': '2023'},
            2: {'league_name': 'Serie B', 'season': '2023'}
        })

    @patch('footy_data_loader.FootyLoader.create_table_if_not_exists')
    @patch('footy_data_loader.FootyLoader.upsert_values')
    def test_insert_league_data(self, mock_upsert_values, mock_create_table_if_not_exists):
        processor = FootyDataProcessor(':memory:')
        leagues = {
            1: {'league_name': 'Serie A', 'season': '2023'},
            2: {'league_name': 'Serie B', 'season': '2023'}
        }
        processor.insert_league_data(leagues)
        mock_upsert_values.assert_has_calls([
            unittest.mock.call('leagues', {'league_id': 1, 'league_name': 'Serie A', 'season': '2023'}, unique_columns=['league_id']),
            unittest.mock.call('leagues', {'league_id': 2, 'league_name': 'Serie B', 'season': '2023'}, unique_columns=['league_id'])
        ])

class TestMainProcessor(unittest.TestCase):
    @patch('footy_processor.FootyDataProcessor.process_country_data')
    @patch('footy_processor.FootyDataProcessor.insert_league_data')
    def test_run(self, mock_insert_league_data, mock_process_country_data):
        mock_process_country_data.return_value = {
            1: {'league_name': 'Serie A', 'season': '2023'},
            2: {'league_name': 'Serie B', 'season': '2023'}
        }
        main = MainProcessor()
        main.run()
        mock_process_country_data.assert_called_once_with('Brazil')
        mock_insert_league_data.assert_called_once_with({
            1: {'league_name': 'Serie A', 'season': '2023'},
            2: {'league_name': 'Serie B', 'season': '2023'}
        })

if __name__ == '__main__':
    unittest.main()