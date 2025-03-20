import json
import gzip
import logging
import aiohttp
import polars as pl
from io import BytesIO
from datetime import datetime, timedelta
from DataPipelines.brokers.base_broker import BaseBroker
from DataPipelines.authentication.upstox_authenticator import UpstoxAuthenticator
from typing import List, Dict, Any


class UpstoxBroker(BaseBroker):
    
    BASE_URL = "https://api.upstox.com/v2"
    BASE_ORDER_URL = "https://api-hft.upstox.com/v2"

    def __init__(self, account_name: str, logger: logging.Logger):
        self.account_name = account_name
        self.broker_name = 'Upstox'
        self.logger = logger
        self.authenticator = UpstoxAuthenticator(account_name=self.account_name, logger=self.logger)
        self.access_token = None
    
    async def initialize(self):
        try:
            self.logger.info(f'Initializing UpstoxBroker for {self.account_name}')
            self.access_token = await self.authenticator.authenticate()
            self.instrument_df = await self.upstox_instruments()
            if self.instrument_df is None:
                raise Exception("Instrument data could not be loaded.")
            return self
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise

    async def upstox_instruments(self) -> pl.DataFrame:
        '''
        Mapping Index tradingsymbols into the upstox master file.
        Add the 'instrument_key' with it's respective 'tradingsymbol'

        '''
        instrument_keys = [
            "NSE_INDEX|Nifty 50", "NSE_INDEX|NIFTY100 EQL Wgt", "NSE_INDEX|NIFTY50 EQL Wgt", "NSE_INDEX|NiftyM150Momntm50",
            "NSE_INDEX|Nifty Auto", "NSE_INDEX|Nifty Commodities", "NSE_INDEX|Nifty Mid Liq 15", "NSE_INDEX|Nifty GS 10Yr Cln",
            "NSE_INDEX|NIFTY TOTAL MKT", "NSE_INDEX|Nifty Serv Sector", "NSE_INDEX|Nifty100 Liq 15", "NSE_INDEX|Nifty Bank",
            "NSE_INDEX|Nifty Next 50", "NSE_INDEX|NIFTY AlphaLowVol", "NSE_INDEX|Nifty Energy", "NSE_INDEX|Nifty Div Opps 50",
            "NSE_INDEX|NIFTY SMLCAP 50", "NSE_INDEX|Nifty PSE", "NSE_INDEX|NIFTY M150 QLTY50", "NSE_INDEX|NIFTY100 LowVol30",
            "NSE_INDEX|Nifty200Momentm30", "NSE_INDEX|Nifty100ESGSecLdr", "NSE_INDEX|Nifty GS 10Yr", "NSE_INDEX|NIFTY Alpha 50",
            "NSE_INDEX|Nifty 500", "NSE_INDEX|Nifty Realty", "NSE_INDEX|NIFTY INDIA MFG", "NSE_INDEX|NIFTY200 QUALTY30",
            "NSE_INDEX|Nifty GrowSect 15", "NSE_INDEX|NIFTY100 ESG", "NSE_INDEX|Nifty GS 8 13Yr", "NSE_INDEX|Nifty Infra",
            "NSE_INDEX|NIFTY SMLCAP 250", "NSE_INDEX|Nifty50 PR 1x Inv", "NSE_INDEX|NIFTY MIDCAP 100", "NSE_INDEX|Nifty FinSrv25 50",
            "NSE_INDEX|NIFTY CONSR DURBL", "NSE_INDEX|India VIX", "NSE_INDEX|Nifty Pharma", "NSE_INDEX|NIFTY MIDCAP 150",
            "NSE_INDEX|Nifty50 TR 1x Inv", "NSE_INDEX|Nifty PSU Bank", "NSE_INDEX|NIFTY HEALTHCARE", "NSE_INDEX|NIFTY500 MULTICAP",
            "NSE_INDEX|Nifty IT", "NSE_INDEX|NIFTY MIDSML 400", "NSE_INDEX|Nifty Media", "NSE_INDEX|Nifty 100",
            "NSE_INDEX|NIFTY100 Qualty30", "NSE_INDEX|NIFTY LARGEMID250", "NSE_INDEX|NIFTY SMLCAP 100", "NSE_INDEX|Nifty Midcap 50",
            "NSE_INDEX|NIFTY MICROCAP250", "NSE_INDEX|Nifty50 PR 2x Lev", "NSE_INDEX|Nifty200 Alpha 30", "NSE_INDEX|Nifty Fin Service",
            "NSE_INDEX|Nifty FMCG", "NSE_INDEX|Nifty50 Value 20", "NSE_INDEX|Nifty50 TR 2x Lev", "NSE_INDEX|Nifty50 Div Point",
            "NSE_INDEX|Nifty MNC", "NSE_INDEX|Nifty Consumption", "NSE_INDEX|Nifty Pvt Bank", "NSE_INDEX|Nifty CPSE",
            "NSE_INDEX|Nifty GS 11 15Yr", "NSE_INDEX|Nifty Metal", "NSE_INDEX|Nifty GS 15YrPlus", "NSE_INDEX|Nifty 200",
            "NSE_INDEX|NIFTY MID SELECT", "NSE_INDEX|Nifty GS Compsite", "NSE_INDEX|Nifty GS 4 8Yr", "NSE_INDEX|NIFTY IND DIGITAL",
            "NSE_INDEX|Nifty Tata 25 Cap", "NSE_INDEX|Nifty Multi Mfg", "NSE_INDEX|NIFTY OIL AND GAS", "NSE_INDEX|Nifty MidSml Hlth",
            "NSE_INDEX|Nifty Multi Infra", "MCX_INDEX|MCXBULLDEX", "BSE_INDEX|INDSTR", "BSE_INDEX|ALLCAP", "BSE_INDEX|SMLCAP",
            "BSE_INDEX|BSEFMC", "BSE_INDEX|REALTY", "BSE_INDEX|SMEIPO", "BSE_INDEX|LRGCAP", "BSE_INDEX|BSE500", "BSE_INDEX|FINSER",
            "BSE_INDEX|AUTO", "BSE_INDEX|BSECD", "BSE_INDEX|MFG", "BSE_INDEX|OILGAS", "BSE_INDEX|UTILS", "BSE_INDEX|GREENX",
            "BSE_INDEX|MIDSEL", "BSE_INDEX|BSEHC", "BSE_INDEX|ENERGY", "BSE_INDEX|SMLSEL", "BSE_INDEX|SENSEX", "BSE_INDEX|MIDCAP",
            "BSE_INDEX|CONDIS", "BSE_INDEX|COMDTY", "BSE_INDEX|BANKEX", "BSE_INDEX|CPSE", "BSE_INDEX|TECK", "BSE_INDEX|POWER",
            "BSE_INDEX|BSECG", "BSE_INDEX|CARBON", "BSE_INDEX|BSE200", "BSE_INDEX|BSE100", "BSE_INDEX|TELCOM", "BSE_INDEX|BSEIPO",
            "BSE_INDEX|BSEIT", "BSE_INDEX|INFRA", "BSE_INDEX|METAL", "BSE_INDEX|BSEPSU", "BSE_INDEX|SENSEX50"
        ]

        tradingsymbol_mappings = {
            'instrument_key': [],
            'trading_symbol': []
        }
        for key in instrument_keys:
            tradingsymbol_mappings['instrument_key'].append(key)
            tradingsymbol_mappings['trading_symbol'].append(key.split('|')[1].strip().upper())
        tradingsymbol_mappings_df = pl.DataFrame(tradingsymbol_mappings)

        instrument_link = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz'

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(instrument_link) as response:
                    if response.status == 200:
                        compressed_data = BytesIO(await response.read())
                        with gzip.GzipFile(fileobj=compressed_data) as f:
                            data = json.load(f)
                            instrument_df = pl.DataFrame(data)
                            instrument_df = instrument_df.join(tradingsymbol_mappings_df, on='instrument_key', how='left')
                            instrument_df = instrument_df.with_columns(
                                pl.when(pl.col("trading_symbol") == '')
                                .then(pl.col("trading_symbol_right"))
                                .otherwise(pl.col("trading_symbol"))
                                .alias("trading_symbol")
                            ).drop("trading_symbol_right")
                            return instrument_df
                    else:
                        error_msg = f"Failed to retrieve Upstox instrument data. Status code: {response.status}"
                        print(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"Error fetching instrument data: {e}")

    async def place_order(self,
                          exchange_token: str,
                          transaction_type: str,
                          quantity: int,
                          product: str,
                          validity: str,
                          order_type: str = 'MARKET',
                          price: float = 0,
                          trigger_price: float = 0,
                          disclosed_quantity: int = 0,
                          is_amo: bool = False,
                          tag: str = '') -> Dict[str, str]:
        """
        Places an order with Upstox.

        Args:
            exchange_token (str): The exchange token of the instrument.
            transaction_type (str): 'BUY' or 'SELL'.
            quantity (int): The quantity to trade.
            product (str): Product code ('I' for Intraday, 'D' for Delivery, etc.).
            validity (str): Validity of the order ('DAY', 'IOC').
            order_type (str, optional): Order type ('MARKET', 'LIMIT', 'SL', 'SL-M'). Defaults to 'MARKET'.
            price (float, optional): Price for LIMIT orders. Defaults to 0.
            trigger_price (float, optional): Trigger price for SL orders. Defaults to 0.
            disclosed_quantity (int, optional): Disclosed quantity. Defaults to 0.
            is_amo (bool, optional): After Market Order flag. Defaults to False.
            tag (str, optional): Order tag. Defaults to ''.

        Returns:
            dict: Order response data.

        Raises:
            ValueError: If the instrument is not found or response data is missing.
            Exception: If order placement fails due to API or response issues.
        """
        try:
            url = f'{self.BASE_ORDER_URL}/order/place'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange_token') == exchange_token)
            )
            if instrument_rows.is_empty():
                error_msg = f"exchange_token: {exchange_token} not found in upstox master file."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            
            instrument_key = instrument_rows['instrument_key'][0]

            payload = {k: v for k, v in {
                'instrument_token': instrument_key,
                'transaction_type': transaction_type,
                'quantity': quantity,
                'product': product,
                'order_type': order_type,
                'price': price,
                'trigger_price': trigger_price,
                'validity': validity,
                'disclosed_quantity': disclosed_quantity,
                'is_amo': is_amo,
                'tag': tag
            }.items() if v is not None}

            async with aiohttp.ClientSession() as session:
                async with session.post(url=url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response.get('status') == 'success':
                            if 'data' in order_response:
                                return order_response['data']
                            else:
                                error_msg = f'Order response data missing for place order: {payload}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'Order not successful. Details: {order_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve order placement response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during order placement: {e}')
            raise
        
    async def modify_order(self,
                           order_id: str,
                           quantity: int = None,
                           price: float = None,
                           order_type: str = None,
                           trigger_price: float = None,
                           disclosed_quantity: int = None,
                           validity: str = None) -> Dict[str, str]:
        """
        Modifies an existing order.

        Args:
            order_id (str): Order ID of the placed order.
            quantity (int, optional): The quantity to modify. Defaults to None.
            price (float, optional): Price for LIMIT orders. Defaults to None.
            order_type (str, optional): Order type ('MARKET', 'LIMIT', etc.). Defaults to None.
            trigger_price (float, optional): Trigger price for SL orders. Defaults to None.
            disclosed_quantity (int, optional): Disclosed quantity. Defaults to None.
            validity (str, optional): Validity of the order ('DAY', 'IOC'). Defaults to None.

        Returns:
            dict: Order response data.

        Raises:
            ValueError: If the order_id is not found or response is missing.
            Exception: If order modification fails due to API or response issues.
        """
        try:
            url = f'{self.BASE_ORDER_URL}/order/modify'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            payload = {k: v for k, v in {
                "quantity": quantity,
                "validity": validity,
                "price": price,
                "order_id": order_id,
                "order_type": order_type,
                "disclosed_quantity": disclosed_quantity,
                "trigger_price": trigger_price
            }.items() if v is not None}

            async with aiohttp.ClientSession() as session:
                async with session.put(url=url, headers=headers, data=json.dumps(payload)) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response.get('status') == 'success':
                            if 'data' in order_response:
                                return order_response['data']
                            else:
                                error_msg = f"Order modification response data missing for modify order: {payload}"
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f"Order modification not successful. Details: {order_response}"
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve order modification response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during order modification: {e}')
            raise    

    async def cancel_order(self, order_id: str) -> Dict[str, str]:
        """
        Cancels an existing order.

        Args:
            order_id (str): The ID of the order to cancel.

        Returns:
            Dict[str, Any]: Dictionary containing details of the canceled order.

        Raises:
            ValueError: If the response is missing required data.
            Exception: If the order cancellation fails due to API or response issues.

        Example:
            cancel_response = await upstox_broker.cancel_order(order_id='12345')
        """
        try:
            url = f'{self.BASE_ORDER_URL}/order/cancel'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            payload = {'order_id': order_id}
            
            async with aiohttp.ClientSession() as session:
                async with session.delete(url=url, headers=headers, data=payload) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response.get('status') == 'success':
                            if 'data' in order_response:
                                return order_response['data']
                            else:
                                error_msg = f'Order response data missing for cancel order: {payload}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'Order cancellation not successful. Details: {order_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve order cancellation response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during order cancellation: {e}')
            raise

    async def get_trade_book(self):
        """Placeholder for trade_book implementation"""
        raise NotImplementedError("trade book not implemented")
        

    async def get_order_details(self, order_id: str) -> Dict[str, Any]: 
        """
        Retrieves details for a specific order from Upstox.

        This function sends a request to Upstox to retrieve details for the specified order ID. It returns a dictionary
        containing the details if the request is successful.

        Args:
            order_id (str): The ID of the order for which details are to be retrieved.

        Returns:
            Dict[str, Any]: A dictionary containing the order details.

        Raises:
            ValueError: If the order details are missing in the response.
            Exception: If retrieving order details fails due to API or response issues.

        Example:
            order_details = await upstox_broker.get_order_details(order_id='12345')
        """
        try:
            url = f'{self.BASE_URL}/order/details'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'order_id': order_id}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response.get('status') == 'success':
                            if 'data' in order_response:
                                return order_response['data']
                            else:
                                error_msg = f'Order response data missing for get order details: {params}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'Retrieving order details unsuccessful. Details: {order_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve order details response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception while retrieving order details: {e}')
            raise

    async def get_orderbook(self) -> List[Dict[str, Any]]:
        """
        Retrieves the current order book from Upstox.

        This function fetches all open orders for the authenticated account and returns them as a list of dictionaries,
        each containing the details of an order.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing an order's details.

        Raises:
            ValueError: If the response is missing expected data.
            Exception: If unable to retrieve order book due to API or response issues.

        Example:
            orderbook = await upstox_broker.get_orderbook()
        """
        try:        
            url = f'{self.BASE_URL}/order/retrieve-all'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response.get('status') == 'success':
                            if 'data' in order_response:
                                return order_response['data']
                            else:
                                error_msg = 'Order response data missing for orderbook retrieval.'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'Retrieving orderbook unsuccessful. Details: {order_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve orderbook: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception occurred while retrieving orderbook: {e}')
            raise

    async def get_funds_and_margin(self, segment: str = None) -> float:
        """
        Retrieves the user's fund and margin details from Upstox.

        Parameters:
            access_token (str): The OAuth access token for authentication.
            segment (str, optional): The market segment to query. Use 'SEC' for Equity or 'COM' for Commodity.
                                    If not specified, the response will include both segments.

        Returns:
            dict: A dictionary containing the fund and margin details.

        Raises:
            Exception: If the API request fails or returns an error.
        """
        try:
            url = f'{self.BASE_URL}/user/get-funds-and-margin'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {}
            if segment:
                params['segment'] = segment

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        self.logger.info(f"Funds and margin details retrieved successfully")
                        balance_response = await response.json()
                        if balance_response.get('status') == 'success':
                            if 'data' in balance_response:
                                return balance_response['data']['equity']['available_margin']
                            else:
                                error_msg = f'Balance response data missing for get funds and margin: {params}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'Retrieving balance unsuccessful. Details: {balance_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve balance response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception while retrieving balance: {e}')

    async def ltp_quote(self, exchange_token: str) -> float:
        """
        Asynchronously retrieves the Last Traded Price (LTP) for a specified exchange token.

        This function fetches the LTP for a given exchange token from Upstox's market quote API. 
        It verifies that the token exists in the local instrument data, then retrieves the price data.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.

        Returns:
            float: The last traded price of the instrument associated with the given exchange token.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if LTP retrieval is unsuccessful due to server issues or invalid responses.
        
        Example:
            exchange_token = '74989'

        Notes:
            - Requires `instrument_df` to contain the exchange token and `instrument_key` mappings.
            - Requires a valid Upstox API access token and active session to function.
        """
        try:
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange_token') == exchange_token)
            )
            if instrument_rows.is_empty():
                error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            url = f'{self.BASE_URL}/market-quote/ltp'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'instrument_key': instrument_key}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        ltp_response = await response.json()
                        if ltp_response['status'] == 'success':
                            if 'data' in ltp_response:
                                data = await self.convert_quote(exchange_tokens=ltp_response['data'])
                                return data[exchange_token]['last_price']
                            else:
                                error_msg = f'LTP response data missing for: {params}'
                                self.logger.error(error_msg)
                                raise ValueError
                        else:
                            error_msg = f'LTP response retrieval unsuccessful. Details: {ltp_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve LTP response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)         
        except Exception as e:
            self.logger.error(f'Exception during LTP response retrieval: {e}')      

    async def ohlc_quote(self, exchange_token: str, interval: str) -> Dict[str, float]:
        """
        Asynchronously retrieves Open-High-Low-Close (OHLC) data for a specified exchange token and time interval.

        This function fetches the OHLC data for a given exchange token from Upstox's market quote API,
        for a specified time interval such as '1day' or '5min'. It verifies that the token exists in 
        the local instrument data before making the request.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.
            interval (str): The time interval for OHLC data (e.g., '1d' (1 day), '1I' (1 minute), '5I' (5 minutes)).

        Returns:
            Dict[str, float]: A dictionary containing the OHLC data, with keys 'open', 'high', 'low', and 'close' 
                              representing respective prices.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if OHLC data retrieval is unsuccessful due to server issues or invalid responses.
        
        Example:
            exchange_token = '74989'
            interval = '1day'
            ohlc_data = await broker.ohlc_quote(exchange_token, interval)
            print(f"OHLC data: {ohlc_data}")
        """
        try:
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange_token') == exchange_token)
            )
            if instrument_rows.is_empty():
                error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            url = f'{self.BASE_URL}/market-quote/ohlc'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'instrument_key': instrument_key, 'interval': interval}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        ohlc_response = await response.json()
                        if ohlc_response.get('status') == 'success':
                            if 'data' in ohlc_response:
                                data = await self.convert_quote(exchange_tokens=ohlc_response['data'])
                                return data[exchange_token]['ohlc']
                            else:
                                error_msg = f'OHLC response data missing for: {params}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'OHLC response retrieval unsuccessful. Details: {ohlc_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve OHLC response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)         
        except Exception as e:
            self.logger.error(f'Exception during OHLC response retrieval: {e}')      

    async def full_market_quote(self, exchange_token: str) -> Dict[str, Any]:
        """
        Asynchronously retrieves the full market quote for a specified exchange token.

        This function fetches comprehensive market data for a given exchange token from Upstox's market 
        quote API. It verifies the existence of the exchange token in the instrument data before making 
        the request, ensuring that only valid tokens are processed.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.

        Returns:
            Dict[str, Any]: A dictionary containing the full market quote data for the specified 
                            exchange token, including details such as bid-ask prices, 
                            last traded price, volume, and other key metrics.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if full market quote retrieval is unsuccessful 
                       due to server issues or invalid responses.

        Example:
            exchange_token = '74989'
        """
        try:
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange_token') == exchange_token)
            )
            if instrument_rows.is_empty():
                error_msg = f'exchange_token: {exchange_token} not found in the upstox master file.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            url = f'{self.BASE_URL}/market-quote/quotes'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'instrument_key': instrument_key}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        quote_response = await response.json()
                        if quote_response.get('status') == 'success':
                            if 'data' in quote_response:
                                data = await self.convert_quote(exchange_tokens=quote_response['data'])
                                return data[exchange_token]
                            else:
                                error_msg = f'Full market quote data missing for: {params}'
                                self.logger.error(error_msg)
                                raise ValueError
                        else:
                            error_msg = f'Full market quote retrieval unsuccessful. Details: {quote_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve full market quote: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)         
        except Exception as e:
            self.logger.error(f'Exception during full market quote retrieval: {e}')      

    async def convert_quote(self, exchange_tokens: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Converts instrument tokens in the quote data to exchange tokens.

        This function takes a dictionary of quote data where the keys are instrument tokens,
        and converts the instrument tokens to their corresponding exchange tokens using the
        instrument DataFrame (`self.instrument_df`). It returns a new dictionary with exchange
        tokens as keys and the original quote data as values.

        Args:
            exchange_tokens (dict): A dictionary where each value contains quote data with an 'instrument_token' key.

        Returns:
            dict: A dictionary with trading symbols as keys and quote data as values.

        Raises:
            Exception: If an error occurs during the conversion process.
        """
        output_dict = {}
        for exchange_token, value in exchange_tokens.items():
            instrument_key = value['instrument_token']
            if instrument_key is None:
                self.logger.error(f"Missing 'instrument_token' in quote data for key {exchange_token}")
                continue
            try:
                temp_df = self.instrument_df.filter(
                    pl.col('instrument_key') == instrument_key
                )
                if temp_df.is_empty():
                    self.logger.error(f"No matching instrument for token {instrument_key}")
                    continue
                new_key = temp_df['exchange_token'][0]
                output_dict[new_key] = value
            except Exception as e:
                self.logger.error(f"Error converting instrument token {instrument_key}: {e}")
                raise
        
        return output_dict  
    
    async def calculate_margin(self,exchange_token, instrument_dict: Dict[str, Any]) -> float:
        """
        Asynchronously calculates the required margin for a specific trading instrument.

        Args:
            instrument_dict (Dict[str, Any]): Dictionary containing the necessary details for calculating margin:
                - exchange_token (str): Unique identifier for the instrument.
                - quantity (int): Number of units to be traded.
                - product (str): Product type, e.g., 'D' for delivery.
                - transaction_type (str): Type of transaction, either 'BUY' or 'SELL'.
                - price (Optional[float]): Price of the instrument. If omitted, the Last Traded Price (LTP) will be used.

        Example:
            instrument_dict = {
                "exchange_token": "011887",
                "quantity": 100,
                "product": "D",
                "transaction_type": "BUY",
                "price": 2300.0  # Optional
            }

        Returns:
            float: The calculated final margin required for the specified instrument.

        Raises:
            ValueError: If the instrument's exchange_token is not found in the Upstox master file or if margin data is missing in the response.
            Exception: For other API-related errors, such as connectivity or response issues, or if margin retrieval fails due to API or response issues.
        """
        try:
            url = f'{self.BASE_URL}/charges/margin'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            quantity = instrument_dict['quantity']
            product = instrument_dict['product']
            transaction_type = instrument_dict['transaction_type']
            instrument_key = exchange_token

            # instrument_rows = self.instrument_df.filter(
            #     (pl.col('exchange_token') == exchange_token)
            # )
            # if instrument_rows.is_empty():
            #     error_msg = f"exchange_token {exchange_token} not found in upstox master file."
            #     self.logger.error(error_msg)
            #     raise ValueError(error_msg)
                
            # instrument_key = instrument_rows['instrument_key'][0]

            instrument_data = {
                'instrument_key': instrument_key,
                'quantity': quantity,
                'transaction_type': transaction_type,
                'product': product,
            }
            instruments = [instrument_data]
            
            payload = {'instruments': instruments}
            async with aiohttp.ClientSession() as session:
                async with session.post(url=url, headers=headers, data=json.dumps(payload)) as response:
                    if response.status == 200:
                        self.logger.info("Margin details retrieved successfully.")
                        margin_response = await response.json()
                        if margin_response.get('status') == 'success':
                            if 'data' in margin_response:
                             
                                return margin_response
                            else:
                                error_msg = f'Margin data missing for: {instrument_dict}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)
                        else:
                            error_msg = f'Margin retrieval unsuccessful. Details: {margin_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve margin data: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)    
        except Exception as e:
            self.logger.error(f'Exception during margin retrieval: {e}')
            raise

    async def calculate_brokerage(self, instrument_dict: Dict[str, Any]) -> float:
        """
        Asynchronously calculates the brokerage for the specified instrument.

        Args:
            instrument_dict (Dict[str, Any]): Dictionary with details for the instrument.
                - 'exchange_token' (str): The exchange token of the instrument.
                - 'quantity' (int): The quantity to trade.
                - 'product' (str): Product type ('D' for Delivery, 'I' for Intraday, etc.).
                - 'transaction_type' (str): Transaction type, either 'BUY' or 'SELL'.
                - 'price' (float, optional): The price per unit for the instrument.

        Example:
            instrument_dict = {
                "exchange_token": "011887",
                "quantity": 100,
                "product": "D",
                "transaction_type": "BUY",
                "price": 2300.0  # Optional
            }                

        Returns:
            float: The total brokerage charges.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails.
        """
        try:
            url = f'{self.BASE_URL}/charges/brokerage'
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            exchange_token = instrument_dict.get('exchange_token')
            quantity = instrument_dict.get('quantity')
            product = instrument_dict.get('product')
            transaction_type = instrument_dict.get('transaction_type')
            price = instrument_dict.get('price', 0)

            if price == 0:
                price = await self.ltp_quote(exchange_token=exchange_token) 
        
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange_token') == exchange_token)
            )
            if instrument_rows.is_empty():
                error_msg = f"exchange_token {exchange_token} not found in upstox master file."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
                
            instrument_key = instrument_rows['instrument_key'][0]

            params = {
                'instrument_token': instrument_key,
                'quantity': quantity,
                'transaction_type': transaction_type,
                'product': product,
                'price': price
            }
    
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        self.logger.info("Brokerage details retrieved successfully.")
                        brokerage_response = await response.json()
                        if brokerage_response.get('status') == 'success':
                            if 'data' in brokerage_response:
                                return brokerage_response['data']['charges']['total']
                            else:
                                error_msg = f'Brokerage data missing for: {instrument_dict}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)     
                        else:
                            error_msg = f'Brokerage retrieval unsuccessful. Details: {brokerage_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve margin data: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)    
        except Exception as e:
            self.logger.error(f'Exception during brokerage retrieval: {e}')

    async def market_holidays(self) -> pl.DataFrame:
        """
        Fetches market holiday information for a specific date.

        This function sends an asynchronous GET request to the market holidays API endpoint to retrieve holiday data
        for the given date. It logs an error and returns None if the API response is unsuccessful or if the holiday
        data is missing. In case of an exception, it logs the exception details and returns None.

        Returns:
            Optional[Dict[str, Any]]: The holiday data as a dictionary if the request is successful and data is available;
                                    otherwise, returns None.
        
        Raises:
            Exception: Logs and raises an exception if there's an error in fetching or processing the data.
        """
        try:
            url = f'{self.BASE_URL}/market/holidays'
            headers = {'Accept': 'application/json'}
            params = {}

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        holiday_response = await response.json()
                        if holiday_response.get('status') == 'success':
                            if 'data' in holiday_response:
                                data = holiday_response['data']

                                expanded_data = []
                                for holiday in data:
                                    date = datetime.strptime(holiday['date'], "%Y-%m-%d")
                                    desscription = holiday['description']
                                    holiday_type = holiday['holiday_type']
                                    closed_exchanges = holiday['closed_exchanges']

                                    for exchange in holiday['open_exchanges']:
                                        start_time_utc = datetime.fromtimestamp(exchange['start_time'] / 1000)
                                        end_time_utc = datetime.fromtimestamp(exchange['end_time'] / 1000)
    
                                        start_time_ist = start_time_utc + timedelta(hours=5, minutes=30)
                                        end_time_ist = end_time_utc + timedelta(hours=5, minutes=30)
                                        
                                        expanded_data.append({
                                            'date': date,
                                            'description': desscription,
                                            'holiday_type': holiday_type,
                                            'closed_exchanges': closed_exchanges,
                                            'exchange': exchange['exchange'],
                                            'start_time_utc': start_time_utc,
                                            'end_time_utc': end_time_utc,
                                            'start_time_ist': start_time_ist,
                                            'end_time_ist': end_time_ist
                                        })
                                df = pl.DataFrame(expanded_data, strict=False)
                                return df
                            else:
                                error_msg = f'Market holiday data missing'
                                self.logger.error(error_msg)
                                return pl.DataFrame(data={}, strict=False)
                        else:
                            error_msg = f'Market holiday data response unsuccessful. Details: {holiday_response}'
                            self.logger.error(error_msg)
                            return pl.DataFrame(data={}, strict=False)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve market holiday data: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        return pl.DataFrame(data={}, strict=False)
        except Exception as e:
            self.logger.error(f'Exception during market holiday response retrieval: {e}')
            return None

    async def historical_data(self,
                              exchange_token: str,
                              interval: str,
                              from_date: str,
                              to_date: str) -> pl.DataFrame:
        """
        Retrieves historical candle data for a specified exchange token from Upstox.

        Args:
            exchange_token (str): The unique identifier for the instrument within the Upstox API.
            interval (str): The time interval for the data (e.g., '1minute', '5minute', '15minute', '1day').
            from_date (str): The starting date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The ending date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: A Polars DataFrame containing historical candle data with columns such as:
                - `datetime`: The timestamp for each data point.
                - `open`: The opening price of the instrument.
                - `high`: The highest price of the instrument.
                - `low`: The lowest price of the instrument.
                - `close`: The closing price of the instrument.
                - `volume`: The trading volume.
                - `oi`: The open interest.

        Raises:
            ValueError: If the `exchange_token` is not found in the Upstox master data.
            Exception: For any errors related to data retrieval or API response issues.

        Example:
            historical_df = await upstox_broker.historical_data(
                exchange_token="12345", 
                interval="1day", 
                from_date="2023-01-01", 
                to_date="2023-06-30"
            )

            This example retrieves daily OHLC data for the instrument with `exchange_token` "12345" from 
            January 1, 2023, to June 30, 2023, and prints the resulting DataFrame.
        """
        try:
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange_token') == exchange_token)
            )
            if instrument_rows.is_empty():
                error_msg = f"exchange_token: {exchange_token} not found in upstox master file."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]  

            url = f'{self.BASE_URL}/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}'
            headers = {
                'Accept': 'application/json'
            }
            params = {
                'instrument_key': instrument_key,
                'interval': interval,
                'from_date': from_date,
                'to_date': to_date
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                    if response.status == 200:
                        hist_response = await response.json()
                        if hist_response.get('status') == 'success':
                            if 'data' in hist_response:
                                
                                data = await self._convert_to_polars_df(data=hist_response['data'],
                                                                        exchange_token=exchange_token,
                                                                        interval=interval,
                                                                        from_date=from_date,
                                                                        to_date=to_date)
                                self.logger.info(f'Historical data for: {exchange_token} from: {from_date} to: {to_date} retrieved.')
                                return data
                            else:
                                error_msg = f'Response data missing for historical data: {params}'
                                self.logger.error(error_msg)
                                raise ValueError(error_msg)

                        else:
                            error_msg = f'Retrieving historical data unsuccessful. Details: {hist_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve historical data: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        return pl.DataFrame(data={}, strict=False)  
        except Exception as e:
            self.logger.error(f'Exception while retrieving historical data: {e}')  
            raise

    async def _convert_to_polars_df(self,
                             data: dict,
                             exchange_token: str,
                             interval: str,
                             from_date: str,
                             to_date: str) -> pl.DataFrame:
        """
        Converts provided candle data to a Polars DataFrame with a timezone-adjusted datetime column.
        
        Args:
            exchange_token (str): The exchange token of the instrument.
            data (dict): Dictionary containing candle data with datetime and OHLC values.
            interval (str): The interval for the historical data (e.g., '1minute', '5minute', '1day').
            from_date (str): The start date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The end date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: Polars DataFrame with adjusted datetime column.
        """
        candles = data.get('candles', [])
        if candles:
            data_dict = {
                "datetime": [item[0] for item in candles],
                "open": [item[1] for item in candles],
                "high": [item[2] for item in candles],
                "low": [item[3] for item in candles],
                "close": [item[4] for item in candles],
                "volume": [item[5] for item in candles],
                "oi": [item[6] for item in candles],
            }
            df = pl.DataFrame(data_dict, strict=False)
            df = df.with_columns(
                pl.col("datetime")
                .str.strptime(pl.Datetime)
                .dt.convert_time_zone("Asia/Kolkata")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            todays_mkt_quote = await self.full_market_quote(exchange_token=exchange_token)
            if todays_mkt_quote['ohlc']:
                row = {
                    'datetime': todays_mkt_quote['timestamp'],
                    'open': todays_mkt_quote['ohlc']['open'],
                    'high': todays_mkt_quote['ohlc']['high'],
                    'low': todays_mkt_quote['ohlc']['low'],
                    'close': todays_mkt_quote['ohlc']['close'],
                    'volume': todays_mkt_quote['volume'],
                    'oi': todays_mkt_quote['oi'],
                }

                todays_df = pl.DataFrame(row)
                todays_df = todays_df.with_columns(
                    pl.col('datetime')
                    .str.strptime(pl.Datetime)
                    .dt.convert_time_zone("Asia/Kolkata")
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                )

                todays_df = todays_df.with_columns([
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Int64),
                    pl.col("oi").cast(pl.Int64)
                ])

                combined_df = pl.concat([df, todays_df])
                combined_df = combined_df.sort('datetime')
                return combined_df
            else:
                self.logger.warning(f"Current day's full market quote not added for exchange token: {exchange_token}.")
                return df
        else:
            self.logger.warning(f"Historical data for exchange token: {exchange_token} from: {from_date} to: {to_date} at interval: {interval} not found.")
            return pl.DataFrame(data={}, strict=False)      
        

    async def expiry_dates(self, **kwargs):
        """Placeholder for Expiry Dates implementation"""
        raise NotImplementedError("expiry_dates not implemented")
    
    async def option_chain(self, **kwargs):
        """Placeholder for option_chain implementation"""
        raise NotImplementedError("option_chain not implemented")
    
    async def master_scrip(self, **kwargs):
        """Placeholder for master_scrip implementation"""
        raise NotImplementedError("master_scrip not implemented")
    
    async def remove_access_token(self) -> None:
        """
        Removes the access_token key from the JSON dictionary.

        Parameters:
            data (dict): The JSON data to process.

        Returns:
            dict: The updated JSON data with access_token removed.
       
        Raises:
            FileNotFoundError: If the tokens.json file does not exist.
            KeyError: If the specified broker or account does not exist in the file.        
        """
        import os
        TOKEN_FILE = 'tokens.json'
        if not os.path.exists(TOKEN_FILE):
            raise FileNotFoundError(f"{TOKEN_FILE} does not exist.") 
        
        try:
            with open(TOKEN_FILE, 'r') as file:
                data = json.load(file)
            
            broker_key = f"{self.broker_name.upper()}_ACCOUNTS"
            if broker_key in data and self.account_name in data[broker_key]:
                if 'access_token' in data[broker_key][self.account_name]:
                    del data[broker_key][self.account_name]['access_token']
                    self.logger.info(f'Access token removed successfully.')
                else:
                    self.logger.warning(f'No access token found.')
            else:
                raise KeyError(f"Broker: '{self.broker_name}' or account: {self.account_name} does not exist in tokens.json file.")
            
            with open (TOKEN_FILE, 'w') as file:
                json.dump(data, file, indent=4)
        except Exception as e:
            self.logger.error(f'An error occured while removing access token: {e}')