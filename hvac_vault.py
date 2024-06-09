import requests
import json
from dotenv import load_dotenv
import os


class HashiCorpAPIClient:
    def __init__(self):
        self.client_id = os.getenv("HCP_CLIENT_ID")
        self.client_secret = os.getenv("HCP_CLIENT_SECRET")
        self.token_url = "https://auth.idp.hashicorp.com/oauth2/token"
        self.api_base_url = "https://api.cloud.hashicorp.com"
        self.access_token = None

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
        
        response = requests.post(self.token_url, data=payload, headers=headers)
        
        if response.status_code == 200:
            self.access_token = response.json().get('access_token')
        else:
            response.raise_for_status()
    
    def _ensure_access_token(self):
        """
        Ensure that we have a valid access token. If not, obtain one.
        """
        if not self.access_token:
            self._get_access_token()
    
    def get_secret(self, organization_id, project_id, app_name):
        """
        Retrieve secret data from HashiCorp Cloud API.
        """
        self._ensure_access_token()
        
        secret_url = f"{self.api_base_url}/secrets/2023-06-13/organizations/{organization_id}/projects/{project_id}/apps/{app_name}/open"
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        
        response = requests.get(secret_url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

# Example usage
if __name__ == "__main__":
    load_dotenv()
    ORGANIZATION_ID = os.getenv("VLT_ORG_ID")
    PROJECT_ID = os.getenv("VLT_PROJECT_ID")
    APP_NAME = os.getenv("VLT_APP")
    
    client = HashiCorpAPIClient()
    
    try:
        secret_data = client.get_secret(ORGANIZATION_ID, PROJECT_ID, APP_NAME)
        print(json.dumps(secret_data, indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
