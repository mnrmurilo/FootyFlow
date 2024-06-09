import os
import requests
import json
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Football Pipeline")

ORGANIZATION_ID = os.getenv("VLT_ORG_ID")
PROJECT_ID = os.getenv("VLT_PROJECT_ID")
APP_NAME = os.getenv("VLT_APP")

if not all([ORGANIZATION_ID, PROJECT_ID, APP_NAME]):
    logger.error("Missing one or more environment variables: VLT_ORG_ID, VLT_PROJECT_ID, VLT_APP")
    raise EnvironmentError("Missing one or more required environment variables.")

class HashiCorpAPIClient:
    def __init__(self):
        self.client_id = os.getenv("HCP_CLIENT_ID")
        self.client_secret = os.getenv("HCP_CLIENT_SECRET")
        self.token_url = "https://auth.idp.hashicorp.com/oauth2/token"
        self.api_base_url = "https://api.cloud.hashicorp.com"
        self.access_token = None

        if not all([self.client_id, self.client_secret]):
            logger.error("Missing HCP_CLIENT_ID or HCP_CLIENT_SECRET")
            raise EnvironmentError("HCP_CLIENT_ID and HCP_CLIENT_SECRET must be set in the environment variables.")

    def _get_access_token(self):
        """
        Obtain the OAuth2 access token using client credentials.
        """
        payload = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials',
            'audience': 'https://api.hashicorp.cloud'
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        logger.info("Requesting new access token")
        response = requests.post(self.token_url, data=payload, headers=headers)

        try:
            response.raise_for_status()
            self.access_token = response.json().get('access_token')
            logger.info("Access token obtained successfully")
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            logger.error(f"An error occurred: {err}")
            raise

    def _ensure_access_token(self):
        """
        Ensure that we have a valid access token. If not, obtain one.
        """
        if not self.access_token:
            logger.info("No access token found, fetching a new one.")
            self._get_access_token()

    def get_secret(self):
        """
        Retrieve secret data from HashiCorp Cloud API.
        """
        self._ensure_access_token()

        secret_url = f"{self.api_base_url}/secrets/2023-06-13/organizations/{ORGANIZATION_ID}/projects/{PROJECT_ID}/apps/{APP_NAME}/open"
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        logger.info(f"Requesting secret from {secret_url}")
        response = requests.get(secret_url, headers=headers)

        try:
            response.raise_for_status()
            secrets = response.json().get('secrets', [])
            if secrets:
                logger.info("Secret retrieved successfully")
                return secrets[0].get('version', {}).get('value')
            else:
                logger.warning("No secrets found in the response")
                return None
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            logger.error(f"An error occurred: {err}")
            raise

# Example usage
if __name__ == "__main__":
    try:
        client = HashiCorpAPIClient()
        secret_data = client.get_secret()
        if secret_data:
            logger.info(f"Secret Data: {secret_data}")
        else:
            logger.warning("No secret data returned")
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {e}")
