import requests

from hvac_vault import HashiCorpAPIClient

client = HashiCorpAPIClient()
secret_data = client.get_secret()