import os
import asyncio
import polars as pl
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException
from DataPipelines.utils.logger import get_logger
from DataPipelines.brokers.base_broker import BaseBroker
from DataPipelines.config import ZERODHA_ACCOUNTS
from DataPipelines.authentication.zerodha_authenticator import ZerodhaAuthenticator


class ZerodhaBroker(BaseBroker):
    
    def __init__(self, account_name: str):
        self.account_name = account_name
        self.broker_name = 'Zerodha'
        self.logger = get_logger(
            main_folder_name='Brokers/ZerodhaBroker',
            broker_name=self.broker_name,
            account_name=f'Accounts/{self.account_name}',
        )
        self.account_config = ZERODHA_ACCOUNTS.get(self.account_name)
        self.authenticator = ZerodhaAuthenticator(account_name=self.account_name)
        self.access_token = None
        self.api = None
        self.instrument_df = None

    async def initialize(self):
        try:     
            self.logger.info('Initializing ZerodhaBroker')
            self.access_token = await self.authenticator.authenticate()
            self.api = KiteConnect(api_key=self.account_config['API_KEY'])
            self.api.set_access_token(access_token=self.access_token)
            instruments_data = self.api.instruments()
            self.instrument_df = pl.DataFrame(instruments_data)
        except Exception as e:
            self.logger.error(f"Initiailzation failed: {e}")
            raise Exception("Initialization failed.")

    async def place_order(self,
                          trading_symbol: str, 
                          exchange: str,
                          transaction_type: str, 
                          order_type: str,
                          quantity: int,
                          product: str,
                          price: float = None,
                          trigger_price: float = None,
                          validity: str = None,
                          tag: str = None,
                          variety: str = 'regular') -> dict:
        """
        Places an order with Zerodha.

        Args:
            trading_symbol (str): The trading symbol.
            exchange (str): Exchange code ('NSE', 'BSE', etc.).
            transaction_type (str): 'BUY' or 'SELL'.
            order_type (str): Order type ('MARKET', 'LIMIT', etc.).
            quantity (int): The quantity to trade.
            product (str): Product code ('CNC', 'MIS', etc.).
            price (float): Price for LIMIT orders.
            trigger_price (float): Trigger price for SL orders.
            validity (str): Validity of the order ('DAY', 'IOC').
            tag (str): Order tag.
            variety (str): Order variety ('regular', 'amo', etc.).

        Returns:
            dict: Order response data.

        Raises:
            Exception: If order placement fails.
            KiteException: If an API error occurs.
        """
        order_params = {
            'tradingsymbol': trading_symbol,
            'exchange': exchange,
            'transaction_type': transaction_type,
            'order_type': order_type,
            'quantity': quantity,
            'product': product,
            'price': price,
            'trigger_price': trigger_price,
            'validity': validity,
            'tag': tag,
            'variety': variety
        }

        try:
            order_response = await asyncio.to_thread(
                self.api.place_order, **order_params
            )
            if order_response['status'] == 'success':
                self.logger.info(f"Order placed successfully: {order_response}")
                return order_response['data']
            else:
                error_msg = f"Order placement failed: {order_response}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except KiteException as e:
            self.logger.error(f"KiteException during order placement: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during order placement: {e}")
            raise

    async def modify_order(self,
                           order_id: str, 
                           quantity: int = None, 
                           price: float = None, 
                           order_type: str = None, 
                           trigger_price: float = None, 
                           validity: str = None, 
                           disclosed_quantity: int = None, 
                           variety: str = 'regular') -> dict:
        """
        Modifies an existing order.

        Args:
            order_id (str): The order ID to modify.
            quantity (int, optional): New quantity.
            price (float, optional): New price.
            order_type (str, optional): New order type.
            trigger_price (float, optional): New trigger price.
            validity (str, optional): New validity.
            disclosed_quantity (int, optional): New disclosed quantity.
            variety (str): Order variety ('regular', 'amo', etc.).

        Returns:
            dict: Order modification response.

        Raises:
            Exception: If order modification fails.
            KiteException: If an API error occurs.
        """
        modify_params = {
            'order_id': order_id,
            'quantity': quantity,
            'price': price,
            'order_type': order_type,
            'trigger_price': trigger_price,
            'validity': validity,
            'disclosed_quantity': disclosed_quantity,
            'variety': variety
        }

        try:
            order_response = await asyncio.to_thread(
                self.api.modify_order, **modify_params
            )
            if order_response['status'] == 'success':
                self.logger.info(f"Order modified successfully: {order_response}")
                return order_response['data']
            else:
                error_msg = f"Order modification failed: {order_response}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except KiteException as e:
            self.logger.error(f"KiteException during order modification: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during order modification: {e}")
            raise

    async def cancel_order(self, order_id: str, variety: str = 'regular') -> dict:
        """
        Cancels an existing order.

        Args:
            order_id (str): The order ID to cancel.
            variety (str): Order variety ('regular', 'amo', etc.).

        Returns:
            dict: Order cancellation response.

        Raises:
            Exception: If order cancellation fails.
            KiteException: If an API error occurs.
        """
        try:
            order_response = await asyncio.to_thread(
                self.api.cancel_order, order_id=order_id, variety=variety
            )
            if order_response['status'] == 'success':
                self.logger.info(f"Order cancelled successfully: {order_response}")
                return order_response['data']
            else:
                error_msg = f"Order cancellation failed: {order_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except KiteException as e:
            self.logger.error(f"KiteException during order cancellation: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during order cancellation: {e}")
            raise

    async def get_order_details(self, order_id: str) -> dict:
        """
        Retrieves order details for a specific order ID.

        Args:
            order_id (str): The order ID.

        Returns:
            dict: Order details.

        Raises:
            Exception: If unable to get order details.
            KiteException: If an API error occurs.
        """
        try:
            order_details = await asyncio.to_thread(
                self.api.order_history, order_id=order_id
            )
            if order_details['status'] == 'success':
                self.logger.info(f"Order details retrieved: {order_details}")
                return order_details
            else:
                error_msg = f"Unable to get order details: {order_details.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)  
        except KiteException as e:
            self.logger.error(f"KiteException during fetching order details: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during fetching order details: {e}")
            raise

    async def get_orderbook(self) -> list:
        """
        Retrieves the current order book.

        Returns:
            list: List of orders.

        Raises:
            Exception: If unable to get order book.
            KiteException: If an API error occurs.
        """
        try:
            orderbook_data = await asyncio.to_thread(
                self.api.orders
            )
            if orderbook_data['status'] == 'success':
                self.logger.info(f"Order book retrieved successfully.")
                return orderbook_data
            else:
                error_msg = f"Unable to get order book: {orderbook_data.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except KiteException as e:
            self.logger.error(f"KiteException during fetching order book: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during fetching order book: {e}")
            raise

    async def get_current_positions(self) -> list:
        """
        Retrieves the current positions.

        Returns:
            list: List of current positions.

        Raises:
            Exception: If unable to get positions.
            KiteException: If an API error occurs.
        """
        try:
            positions = await asyncio.to_thread(
                self.api.positions
            )
            if positions['status'] == 'success':
                self.logger.info(f"Current positions retrieved successfully.")
                return positions['data']['net']
        except KiteException as e:
            self.logger.error(f"KiteException during fetching positions: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during fetching positions: {e}")
            raise

    async def ltp_quote(self,
                        tradingsymbol_list: dict) -> dict:
        """
        Get last traded price for dict of stocks.

        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).

        Example: 
            trading_symbol_list = {
                            RELIANCE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE : {
                                'exchange' : 'BSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE24DEC1100CE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'FO'
                                }
                            }
            
        Returns:
            dict: Last traded price data.

        Raises:
            Exception: If getting LTP quote fails.
            KiteException: If an API error occurs.
        """
        zerodha_tradingsymbol_list = []
        for tradingsymbol in tradingsymbol_list:
            exchange = tradingsymbol['exchange']
            instrument_string = f'{exchange}:{tradingsymbol}'
            zerodha_tradingsymbol_list.append(instrument_string)
        
        try:
            ltp_response = await asyncio.to_thread(
                self.api.ltp, instruments=zerodha_tradingsymbol_list
            )
            if ltp_response:
                return ltp_response
            else:
                error_msg = f"LTP response failed: {ltp_response}"
                self.logger.error(error_msg)
                raise Exception(error_msg)  
        except KiteException as e:
            self.logger.error(f"KiteException while getting LTP quote: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting LTP quote: {e}")
            raise

    async def ohlc_quote(self,
                        tradingsymbol_list: dict) -> dict:
        """
        Get OHLC price for dict of stocks.

        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).

        Example: 
            trading_symbol_list = {
                            RELIANCE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE : {
                                'exchange' : 'BSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE24DEC1100CE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'FO'
                                }
                            }
            
        Returns:
            dict: Last traded price data.

        Raises:
            Exception: If getting OHLC quote fails.
            KiteException: If an API error occurs.
        """
        zerodha_tradingsymbol_list = []
        for tradingsymbol in tradingsymbol_list:
            exchange = tradingsymbol['exchange']
            instrument_string = f'{exchange}:{tradingsymbol}'
            zerodha_tradingsymbol_list.append(instrument_string)
        
        try:
            ohlc_response = await asyncio.to_thread(
                self.api.ohlc, instruments=zerodha_tradingsymbol_list
            )
            if ohlc_response['status'] == 'success':
                return ohlc_response['data']
            else:
                error_msg = f"OHLC response failed: {ohlc_response}"
                self.logger.error(error_msg)
                raise Exception(error_msg)  
        except KiteException as e:
            self.logger.error(f"KiteException while getting OHLC quote: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting OHLC quote: {e}")
            raise

    async def full_market_quote(self,
                        tradingsymbol_list: dict) -> dict:
        """
        Get full market quote including limit order depth for dict of stocks.

        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).

        Example: 
            trading_symbol_list = {
                            RELIANCE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE : {
                                'exchange' : 'BSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE24DEC1100CE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'FO'
                                }
                            }
            
        Returns:
            dict: full market quote data.

        Raises:
            Exception: If getting full market quote fails.
            KiteException: If an API error occurs.
        """
        zerodha_tradingsymbol_list = []
        for tradingsymbol in tradingsymbol_list:
            exchange = tradingsymbol['exchange']
            instrument_string = f'{exchange}:{tradingsymbol}'
            zerodha_tradingsymbol_list.append(instrument_string)
        
        try:
            full_mkt_response = await asyncio.to_thread(
                self.api.quote, instruments=zerodha_tradingsymbol_list
            )
            if full_mkt_response['status'] == 'success':
                return full_mkt_response['data']
            else:
                error_msg = f"full market response failed: {full_mkt_response}"
                self.logger.error(error_msg)
                raise Exception(error_msg)  
        except KiteException as e:
            self.logger.error(f"KiteException while getting full market quote: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting full market quote: {e}")
            raise