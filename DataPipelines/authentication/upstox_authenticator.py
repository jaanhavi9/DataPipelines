import os
import time
import pyotp
import requests
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from urllib.parse import urlparse, parse_qs
from app.authentication.base_authenticator import BaseAuthenticator
from app.config import UPSTOX_ACCOUNTS
import json

TOKEN_FILE = 'tokens.json'

class UpstoxAuthenticator(BaseAuthenticator):
   
    def __init__(self, account_name: str, logger: logging.Logger):
        self.account_name = account_name
        self.broker_name = 'Upstox'
        self.account_config = UPSTOX_ACCOUNTS.get(self.account_name)
        self.api_key = self.account_config['API_KEY']
        self.api_secret = self.account_config['API_SECRET']
        self.redirect_uri = self.account_config['REDIRECT_URL']
        self.phone_no = self.account_config['PHONE_NO']
        self.totp_key = self.account_config['TOTP_KEY']
        self.pin_code = self.account_config['PIN_CODE']
        self.logger = logger
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
            self.logger.info('Starting authentication process for Upstox')
            self._load_access_token()
            if not self.access_token:
                auth_code = self._perform_login()
                if auth_code:
                    self.access_token = self._get_access_token(auth_code)
                    self._save_access_token()
                else:
                    self.logger.error('Failed to obtain authentication code.')
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
                data = {'UPSTOX_ACCOUNTS': {}}

            data['UPSTOX_ACCOUNTS'][self.account_name] = token_data

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
                account_data = data['UPSTOX_ACCOUNTS'].get(self.account_name)
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
        Performs the login process using Selenium automation to obtain the authorization code.

        Returns:
            str: The authorization code obtained after login.

        Raises:
            Exception: If login fails.
        """
        auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={self.api_key}&redirect_uri={self.redirect_uri}"

        self.logger.info('Creating WebDriver for Upstox login')

        try:
            self.driver = self._create_webdriver()
            self.driver.get(auth_url)
            self.logger.info('Opened Upstox login page')

            self._enter_phone_number()
            self._enter_totp()
            self._enter_pin_code()

            time.sleep(10)
            current_url = self.driver.current_url
            self.logger.info(f'Current URL after login: {current_url}')
            auth_code = self._get_code_from_url(current_url)
            return auth_code
        except Exception as e:
            self.logger.error(f'Error during Upstox login: {e}')
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
        chrome_options = Options()
        chromium_location = '/usr//bin/google-chrome'
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
                service = ChromeService("/usr/local/bin/chromedriver")
                driver = webdriver.Chrome(service=service, options=chrome_options)
                self.logger.info("WebDriver created successfully")
                return driver
            except Exception as e:
                self.logger.error(f"WebDriver creation attempt {attempt+1} failed: {e}")
                time.sleep(5)
        self.logger.error("Failed to create WebDriver after multiple attempts")
        raise Exception("Failed to create WebDriver after multiple attempts")

    def _enter_phone_number(self):
        """
        Enters the phone number on the login page and requests OTP.
        """
        time.sleep(5)
        self.logger.info('Entering phone number')
        mobilenum = WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#mobileNum"))
        )

        mobilenum.clear()
        mobilenum.send_keys(self.phone_no)
        get_otp_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#getOtp'))
        )
        get_otp_button.click()
        self.logger.info('Phone number entered and OTP requested')

    def _enter_totp(self):
        """
        Enters the TOTP code generated from the TOTP key.
        """
        self.logger.info('Entering TOTP')
        otp_input = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#otpNum'))
        )
        otp_input.clear()
        totp = pyotp.TOTP(self.totp_key)
        otp_input.send_keys(totp.now())
        continue_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#continueBtn'))
        )
        continue_button.click()
        self.logger.info('TOTP entered and continued')

    def _enter_pin_code(self):
        """
        Enters the 6-digit PIN code to complete the login process.
        """
        self.logger.info('Entering 6-digit PIN code')
        pin_input = WebDriverWait(self.driver, 40).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#pinCode'))
        )
        pin_input.send_keys(self.pin_code)
        pin_continue_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#pinContinueBtn'))
        )
        pin_continue_button.click()
        self.logger.info('PIN code entered and continued')

    def _get_code_from_url(self, url: str) -> str:
        """
        Extracts the authorization code from the redirected URL.

        Args:
            url (str): The URL containing the authorization code.

        Returns:
            str: The extracted authorization code.

        Raises:
            Exception: If the code cannot be extracted from the URL.
        """
        try:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            code = query_params.get('code', [None])[0]
            self.logger.info(f'Authorization code obtained: {code}')
            return code
        except Exception as e:
            self.logger.error('Could not fetch code from the URL')
            raise

    def _get_access_token(self, code):
        """
        Exchanges the authorization code for an access token.

        Args:
            code (str): The authorization code obtained after login.

        Returns:
            str: Access token.

        Raises:
            Exception: If the access token cannot be obtained.
        """
        self.logger.info('Exchanging authorization code for access token')
        url = 'https://api.upstox.com/v2/login/authorization/token'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        data = {
            'code': code,
            'client_id': self.api_key,
            'client_secret': self.api_secret,
            'redirect_uri': self.redirect_uri,
            'grant_type': 'authorization_code',
        }

        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            self.logger.info('Access token obtained successfully')

            access_token = token_data['access_token']
            return access_token
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
            raise
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")
            raise
