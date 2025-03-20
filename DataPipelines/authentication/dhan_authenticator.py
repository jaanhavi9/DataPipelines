import os
import json
import logging
from dhanhq import dhanhq
from DataPipelines.authentication.base_authenticator import BaseAuthenticator
from DataPipelines.utils.logger import setup_logger
from DataPipelines.config import DHAN_ACCOUNTS

TOKEN_FILE = 'tokens.json'

class DhanAuthenticator(BaseAuthenticator):
    
    def __init__(self, account_name: str):
        self.account_name = account_name
        self.account_config = DHAN_ACCOUNTS.get(self.account_name)
        if not self.account_config:
            raise ValueError(f"Account configuration not found for {account_name}")
        self.client_id = self.account_config['CLIENT_ID']
        self.access_token = self.account_config.get('ACCESS_TOKEN')

    async def authenticate(self):
        """
        Simple authentication using stored credentials
        """
        if not self.client_id or not self.access_token:
            raise ValueError("Missing CLIENT_ID or ACCESS_TOKEN in configuration")
        
        return self.client_id, self.access_token

    def get_auth_token(self):
        """
        Retrieves the current access token.

        Returns:
            str: The access token.
        """
        return self.access_token
    
    def get_client(self):
        """
        Returns the authenticated Dhan client.
        
        Returns:
            dhanhq: The authenticated Dhan client.
        """
        if not self.dhan_client:
            self.dhan_client = dhanhq(self.client_id, self.access_token)
        return self.dhan_client

    def _verify_token(self):
        """
        Verifies that the access token is valid by making a simple API call.
        
        Raises:
            Exception: If the token verification fails.
        """
        try:
            # Make a simple API call to verify the token
            # For example, get user profile or account balance
            # Adjust this based on Dhan API capabilities
            self.dhan_client.get_user_details()
            self.logger.info('Access token verified successfully')
        except Exception as e:
            self.logger.error(f'Token verification failed: {e}')
            self.access_token = None
            raise Exception(f'Invalid access token: {str(e)}')

    def _save_access_token(self):
        """
        Saves the access token to a JSON file.
        """
        token_data = {
            'access_token': self.access_token,
        }
        try:
            if os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE, 'r') as f:
                    data = json.load(f)
            else:
                data = {'DHAN_ACCOUNTS': {}}

            if 'DHAN_ACCOUNTS' not in data:
                data['DHAN_ACCOUNTS'] = {}

            data['DHAN_ACCOUNTS'][self.account_name] = token_data

            with open(TOKEN_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            self.logger.info('Access token saved to tokens.json')
        except Exception as e:
            self.logger.error(f"Error saving access token: {e}")

    def _load_access_token(self):
        """
        Loads the access token from a JSON file if available.
        """
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                account_data = data.get('DHAN_ACCOUNTS', {}).get(self.account_name)
                if account_data:
                    self.access_token = account_data['access_token']
                    self.logger.info('Access token loaded from tokens.json')
                else:
                    self.access_token = None
        except FileNotFoundError:
            self.logger.info('tokens.json file not found')
            self.access_token = None
        except Exception as e:
            self.logger.error(f"Error loading access token: {e}")
            self.access_token = None