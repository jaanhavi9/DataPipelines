import os
import time
import json
import pyotp
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException
from authentication.base_authenticator import BaseAuthenticator
from utils.logger import get_logger
from config import ZERODHA_ACCOUNTS

TOKEN_FILE = 'tokens.json'

class ZerodhaAuthenticator(BaseAuthenticator):
    
    def __init__(self, account_name: str):
        self.account_name = account_name
        self.broker_name = 'Zerodha'
        self.account_config = ZERODHA_ACCOUNTS.get(self.account_name)
        self.api_key = self.account_config['API_KEY']
        self.api_secret = self.account_config['API_SECRET']
        self.username = self.account_config['USERID']
        self.password = self.account_config['PASSWD']
        self.totp_key = self.account_config['TOTP_KEY']
        self.logger = get_logger(
            main_folder_name=f'Authenticators/ZerodhaAuthenticator/Accounts/{self.account_name}',
            account_name=self.account_name,
            broker_name=self.broker_name
        )
        self.access_token = None
        self.driver = None

    async def authenticate(self):
        """
        Performs authentication process to obtain the access token.

        Returns:
            str: Access token if authentication is successful.

        Raises:
            Exception: If authentication fails.
        """
        if not self.access_token:
            self.logger.info('Starting authentication process for Zerodha')
            self._load_access_token()
            if not self.access_token:
                request_token = self._perform_login()
                if request_token:
                    self.access_token = self._get_access_token(request_token)
                    self._save_access_token()
                else:
                    self.logger.error('Failed to obtain request token.')
                    raise Exception('Authentication failed')
        return self.access_token

    def get_auth_token(self):
        """
        Retrieves the current access token.

        Returns:
            str: The access token.
        """
        return self.access_token

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
                data = {'ZERODHA_ACCOUNTS': {}}

            if 'ZERODHA_ACCOUNTS' not in data:
                data['ZERODHA_ACCOUNTS'] = {}

            data['ZERODHA_ACCOUNTS'][self.account_name] = token_data

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
                account_data = data.get('ZERODHA_ACCOUNTS', {}).get(self.account_name)
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

    def _perform_login(self):
        """
        Performs the login process using Selenium automation to obtain the request token.

        Returns:
            str: The request token obtained after login.

        Raises:
            Exception: If login fails.
        """
        kite = KiteConnect(api_key=self.api_key)
        login_url = kite.login_url()
        self.logger.info('Creating WebDriver for Zerodha login')

        try:
            self.driver = self._create_webdriver()
            self.driver.get(login_url)
            self.logger.info('Opened Zerodha login page')

            self._enter_username()
            self._enter_password()
            self._enter_totp()

            current_url = self.driver.current_url
            self.logger.info(f'Current URL after login: {current_url}')
            request_token = self._get_request_token_from_url(current_url)
            return request_token
        except Exception as e:
            self.logger.error(f'Error during Zerodha login: {e}')
            raise
        finally:
            if self.driver:
                self.driver.quit()

    def _create_webdriver(self):
        """
        Creates and configures a Selenium WebDriver instance.

        Returns:
            WebDriver: Configured Selenium WebDriver instance.

        Raises:
            Exception: If WebDriver creation fails after multiple attempts.
        """
        chrome_options = webdriver.ChromeOptions()
        chromium_location = '/usr/bin/chromium'
        chromedriver_location = '/usr/bin/chromedriver'
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.binary_location = chromium_location

        for attempt in range(5):
            try:
                driver = webdriver.Chrome(service=ChromeService(executable_path=chromedriver_location), options=chrome_options)
                self.logger.info("WebDriver created successfully")
                return driver
            except Exception as e:
                self.logger.error(f"WebDriver creation attempt {attempt+1} failed: {e}")
                time.sleep(5)
        self.logger.error("Failed to create WebDriver after multiple attempts")
        raise Exception("Failed to create WebDriver after multiple attempts")

    def _enter_username(self):
        """
        Enters the username on the login page.
        """
        self.logger.info('Entering username')
        username_input = WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[id='userid']"))
        )
        username_input.clear()
        username_input.send_keys(self.username)
        self.logger.info('Username entered')

    def _enter_password(self):
        """
        Enters the password on the login page and submits the form.
        """
        self.logger.info('Entering password')
        password_input = WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[id='password']"))
        )
        password_input.clear()
        password_input.send_keys(self.password)
        login_button = self.driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        login_button.click()
        self.logger.info('Password entered and login submitted')

    def _enter_totp(self):
        """
        Enters the TOTP code generated from the TOTP key.
        """
        self.logger.info('Entering TOTP')
        totp_input = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[label='External TOTP']"))
        )
        totp_input.clear()
        totp = pyotp.TOTP(self.totp_key)
        totp_input.send_keys(totp.now())
        self.logger.info('TOTP entered')
        time.sleep(10)

    def _get_request_token_from_url(self, url: str) -> str:
        """
        Extracts the request token from the redirected URL.

        Args:
            url (str): The URL containing the request token.

        Returns:
            str: The extracted request token.

        Raises:
            Exception: If the request token cannot be extracted from the URL.
        """
        try:
            sp = url.split("&")
            cut_dict = {}
            for x in sp:
                cut = x.split("=")
                cut_dict[cut[0]] = cut[1]
            request_token = cut_dict.get('request_token', None)
            self.logger.info(f'Request token obtained: {request_token}')
            return request_token
        except Exception as e:
            self.logger.error('Could not fetch request token from the URL')
            raise

    def _get_access_token(self, request_token):
        """
        Exchanges the request token for an access token.

        Args:
            request_token (str): The request token obtained after login.

        Returns:
            str: The access token.

        Raises:
            Exception: If the access token cannot be obtained.
        """
        self.logger.info('Exchanging request token for access token')
        kite = KiteConnect(api_key=self.api_key)
        try:
            data = kite.generate_session(request_token, api_secret=self.api_secret)
            access_token = data['access_token']
            self.logger.info('Access token obtained successfully')
            return access_token
        except KiteException as e:
            self.logger.error(f"Error generating session: {e}")
            raise
