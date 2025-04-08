import polars as pl
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from DataPipelines.brokers.base_broker import BaseBroker
from DataPipelines.authentication.dhan_authenticator import DhanAuthenticator
import aiohttp
import pandas as pd
import json

COMPACT_CSV_URL = 'https://images.dhan.co/api-data/api-scrip-master.csv'
DETAILED_CSV_URL = 'https://images.dhan.co/api-data/api-scrip-master-detailed.csv'

class DhanBroker(BaseBroker):

    def __init__(self, account_name: str, logger: logging.Logger):
        super().__init__(account_name)
        self.broker_name = 'Dhan'
        self.authenticator = DhanAuthenticator(account_name=self.account_name)
        self.client_id = None 
        self.access_token = None
        self.base_url = "https://api.dhan.co"
        self.logger = logger

    async def initialize(self):
        """Initialize with credentials"""
        try:
            self.client_id, self.access_token = await self.authenticator.authenticate()

            self.logger.info(f'Initialized DhanBroker for {self.account_name}')
            return self
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise

    async def historical_data(self, exchange_token: str, start_date: str, end_date: str):
        try:
            url = self.base_url + "/charts/historical"
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            exchange, token = exchange_token.split(':')
            token = int(token)  

            request_data = {
                "securityId": token,
                "exchangeSegment": exchange,
                "instrument": "EQUITY",
                "fromDate": start_date,
                "toDate": end_date,
                "interval": "1d"
            }

            async with aiohttp.ClientSession() as session:

                async with session.post(
                    f"{self.base_url}/v2/charts/historical",
                    headers=headers,
                    json=request_data


                ) as response:
                    data = await response.json()
                    return data

        except Exception as e:
            self.logger.error(f"Error getting LTP: {e}")
            raise
    
    async def master_scrip(self, mode = 'compact'):
        '''
        Downloads the Master Scrip file for Dhan and returns a dataframe of it.
        '''

        filename = 'security_id_list.csv'

        try:
            if mode == "compact":
                csv_url = COMPACT_CSV_URL
            elif mode == 'detailed':
                csv_url = DETAILED_CSV_URL
            else:
                raise ValueError("Invalid Mode - Chose compact or detailed")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(csv_url) as response:
                    response.raise_for_status()

                    with open (filename, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            f.write(chunk)

            df = pd.read_csv(filename)
            return df
        
        except Exception as e:
            print(e)


    async def get_trade_book(self):
        '''
        Gets the array of all executed orders for the day

        Return: list
                [
            {
                "dhanClientId": "1000000009",
                "orderId": "112111182045",
                "exchangeOrderId": "15112111182045",
                "exchangeTradeId": "15112111182045",
                "transactionType": "BUY",
                "exchangeSegment": "NSE_EQ",
                "productType": "INTRADAY",
                "orderType": "LIMIT",
                "tradingSymbol": "TCS",
                "securityId": "11536",
                "tradedQuantity": 40,
                "tradedPrice": 3345.8,
                "createTime": "2021-03-10 11:20:06",
                "updateTime": "2021-11-25 17:35:12"
                "exchangeTime": "2021-11-25 17:35:12",
                "drvExpiryDate": null,
                "drvOptionType": null,
                "drvStrikePrice": 0.0
            }
        ]


        '''
        try:
            url = self.base_url + "/trades"
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response:
                            return order_response
                    
                        else:
                            self.logger.info("No orders executed for the day")
                            return []

                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve Trade Book response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during fetching trade book - {e}')
            raise Exception(e)


    async def get_order_details(self, order_id:str):
        '''
        This API lets you retrieve an array of all orders requested in a day with their last updated status.

        Return: list
        '''
        try:
             url = self.base_url + f"/orders/{order_id}"
             headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }
             async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers) as response:
                    if response.status == 200:
                        order_response = await response.json()
                        if order_response.get('orderStatus') == 'TRADED':

                            #rename correlationId to tag for consistency across all brokers
                            order_response['tag'] = order_response.pop('correlationId')
                            return order_response
                    
                        else:
                            error_msg = f'Order not successful. Details: {order_response}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve order ID response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during fetching order details by order ID: {e}')
            raise

    
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
        
        '''
        The order request API lets you place new orders. Also uses the get_order_by_order_id function to fetch all the data related to the order. 

        Return: dict

            {
                "orderId": "112111182198",
                "orderStatus": "PENDING",
            }

        '''
        
        try:
            url = self.base_url + '/orders'
            exchange_segment, security_id = exchange_token.split(":")
       
            payload = {
                "dhanClientId": self.client_id,
                "correlationId": tag,
                "transactionType": transaction_type.upper(),
                "exchangeSegment": exchange_segment.upper(),
                "productType": product.upper(),
                "orderType": order_type.upper(),
                "validity": validity.upper(),
                "securityId": security_id,
                "quantity": int(quantity),
                "disclosedQuantity": int(disclosed_quantity),
                "price": float(price),
                "afterMarketOrder": is_amo,
                "boProfitValue": "",
                "boStopLossValue": ""
            }

            if trigger_price > 0:
                payload["triggerPrice"] = float(trigger_price)
            elif trigger_price == 0:
                payload["triggerPrice"] = 0.0

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url=url, headers=headers, data=payload) as response:
               
                    if response.status == 200:
                  
                        order_response = await response.json()
                        if order_response.get('orderStatus') == 'TRADED':
                            order_id = order_response.get('orderId')
                            order_response = await self.get_order_details(order_id=order_id)
                            return order_response
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


    async def cancel_order(self, order_id: str):
        """Placeholder for cancel_order implementation"""
        raise NotImplementedError("cancel_order not implemented")

    async def modify_order(self, **kwargs):
        """Placeholder for modify_order implementation"""
        raise NotImplementedError("modify_order not implemented")


    async def get_orderbook(self):
        '''
        This API lets you retrieve an array of all orders requested in a day with their last updated status.

        Return: list 
        '''

        try:
            url = self.base_url + '/orders'
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url = url, headers = headers) as response:
                    if response.status == 200:
                        order_book = await response.json()
                        if order_book:
                            return order_book
                        
                        else:
                            error_msg = f"No order book details available - {order_book}"
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                        
                    else:
                        error_text = await response.text()
                        error_msg = f"Failed to retreive order book details: Response - {response.status} - {error_text}"
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        
        except Exception as e:
            self.logger.error(f"Exception during fetching order book data - {e}")
            raise



    async def get_funds_and_margin(self, segment: str = None):
        try:
            url = self.base_url + "/fundlimit"
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers) as response:
                    if response.status == 200:
                        fund_details = await response.json()
                        if fund_details:
                            return fund_details
                    
                        else:
                            error_msg = f'No Fund Details. Details: {fund_details}'
                            self.logger.error(error_msg)
                            raise Exception(error_msg)
                    else:
                        error_text = await response.text()
                        error_msg = f'Failed to retrieve Fund Details Response: {response.status} - {error_text}'
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
        except Exception as e:
            self.logger.error(f'Exception during fetching fund details - {e}')
            raise

    async def market_holidays(self):
        """Placeholder for market_holidays implementation"""
        raise NotImplementedError("market_holidays not implemented")


    async def ltp_quote(self, exchange_token: str):
        try:
            
            exchange, token = exchange_token.split(':')
            token = int(token)  
            
            request_data = {
                exchange: [token]  
            }
            
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json', 
                'access-token': self.access_token,
                'client-id': self.client_id
            }
            
            async with aiohttp.ClientSession() as session:

                async with session.post(
                    f"{self.base_url}/v2/marketfeed/ltp",
                    headers=headers,
                    json=request_data

                ) as response:
                    data = await response.json()

                    if data.get('status') == 'success':
                        # Convert token back to string for response parsing
                        return data['data'][exchange][str(token)]['last_price']
                    else:
                        raise Exception(f"Failed to get LTP: {data}")
                    
        except Exception as e:
            self.logger.error(f"Error getting LTP: {e}")
            raise

    async def ohlc_quote(self, exchange_token: str):
        """
        Fetch OHLC data using direct API endpoint 
        Example response: {
            "data": {
                "NSE_EQ": {
                    "11536": {
                        "last_price": 4525.55,
                        "ohlc": {
                            "open": 4521.45,
                            "close": 4507.85,
                            "high": 4530,
                            "low": 4500
                        }
                    }
                }
            }
        }
        """
        try:
            exchange, token = exchange_token.split(':')
            
            request_data = {
                exchange: [int(token)]
            }

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/v2/marketfeed/ohlc",
                    headers=headers,
                    json=request_data
                ) as response:
                    data = await response.json()
                    
                    if data.get('status') == 'success':
                        return data['data'][exchange][token]
                    else:
                        raise Exception(f"Failed to get OHLC data: {data}")

        except Exception as e:
            self.logger.error(f"Error getting OHLC data: {e}")
            raise

    async def expiry_dates(self,  exchange_token: str):
        """
        Fetch expiry dates for a given stock or index
        """
        try:
            exchange, token = exchange_token.split(':')
            
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            params = {
                'UnderlyingScrip': int(token),
                'UnderlyingSeg': exchange
            }

            self.logger.info(f"Requesting expiry dates for {exchange_token} with payload: {params}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/v2/optionchain/expirylist",
                    headers=headers,
                    json = params
                ) as response:
                    data = await response.json()
                    
                    if data.get('status') == 'success':
                        return data['data']
                    else:
                        raise Exception(f"Failed to fetch expiry dates: {data}")

        except Exception as e:
            self.logger.error(f"Error fetching expiry dates: {e}")
            raise

    async def option_chain(self, exchange_token: str, expiry_date: str = None):
        '''
        {
    "data": {
        "last_price": 24964.25,
        "oc": {
            .
            .
            "25000.000000": {
                "ce": {
                    "greeks": {
                        "delta": 0.52546,
                        "theta": -12.88756,
                        "gamma": 0.00136,
                        "vega": 12.98931
                    },
                    "implied_volatility": 8.945204889199001,
                    "last_price": 125.05,
                    "oi": 5962675,
                    "previous_close_price": 190.45,
                    "previous_oi": 3939375,
                    "previous_volume": 831463,
                    "top_ask_price": 124.9,
                    "top_ask_quantity": 1000,
                    "top_bid_price": 124,
                    "top_bid_quantity": 100,
                    "volume": 84202625
                },
                "pe": {
                    "greeks": {
                        "delta": -0.48099,
                        "theta": -10.56587,
                        "gamma": 0.00092,
                        "vega": 13.00105
                    },
                    "implied_volatility": 13.321804909313869,
                    "last_price": 165,
                    "oi": 5059700,
                    "previous_close_price": 153.6,
                    "previous_oi": 4667700,
                    "previous_volume": 1047989,
                    "top_ask_price": 165,
                    "top_ask_quantity": 375,
                    "top_bid_price": 164.05,
                    "top_bid_quantity": 50,
                    "volume": 81097175
                }
            }
            .
            .
            .
        }
    }
}
        '''
       
        try:
            exchange, token = exchange_token.split(':')

            if not expiry_date:
                expiry_dates = await self.expiry_dates(exchange_token)
                if not expiry_dates:
                    raise Exception("No expiry dates found.")
                expiry_date = expiry_dates[1] 
                self.logger.info(f"Requesting option chain for {token} for expiry {expiry_date} ") 

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            params = {
                'UnderlyingScrip': int(token),
                'UnderlyingSeg': exchange,
                'Expiry': str(expiry_date)
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/v2/optionchain",
                    headers=headers,
                    json = params
                ) as response:
                    data = await response.json()
                    
                    if data.get('status') == 'success':
                        return data['data']
                    else:
                        raise Exception(f"Failed to fetch option chain: {data}")

        except Exception as e:
            self.logger.error(f"Error fetching option chain: {e}")
            raise


    async def full_market_quote(self, exchange_token: str):
        """
        Fetch full market depth using direct API endpoint
        Returns complete market depth including buy/sell orders
        """
        try:
            exchange, token = exchange_token.split(':')
            
            request_data = {
                exchange: [int(token)]
            }

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/v2/marketfeed/quote",
                    headers=headers, 
                    json=request_data
                ) as response:
                    data = await response.json()
                    
                    if data.get('status') == 'success':
                        return data['data'][exchange][token]
                    else:
                        raise Exception(f"Failed to get market depth: {data}")

        except Exception as e:
            self.logger.error(f"Error getting market depth: {e}")
            raise

    async def calculate_brokerage(self, exchange_token: str, instrument_dict: str):
        try:
            margin = await self.calculate_margin(exchange_token, instrument_dict)
            if margin:
                brokerage = margin['brokerage']
                if brokerage:
                    return brokerage
                else:
                    error = "Brokerage not present"
                    self.logger.error(error)
            else:
                error = f"unable to get brokerage for {exchange_token}"
                self.logger.error(error)
        except Exception as e:
            error = f"Exception while getting brokerage for {exchange_token} - {e}"
            self.logger.error(error)

    async def calculate_margin(self, exchange_token:str, instrument_dict: dict):
        try:
            print("hello from  dhan broker")
            exchange, token = exchange_token.split(':')
            transaction_type = instrument_dict['transaction_type']
            quantity = instrument_dict['quantity']
            product_type = instrument_dict['product_type']
            price = instrument_dict['price']
            trigger_price = instrument_dict['trigger_price']
            
            url = f"{self.base_url}/v2/margincalculator"

            params = {
                "dhanClientId": self.client_id,
                "exchangeSegment": str(exchange),
                "transactionType": transaction_type,
                "quantity": quantity,
                "productType": product_type,
                "securityId": int(token),
                "price": price,
            }
    

            if trigger_price > 0:
                params['triggerPrice'] = float(trigger_price)
            elif trigger_price == 0:
                params['triggerPrice'] = 0.0

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'access-token': self.access_token,
                'client-id': self.client_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, 
                    headers = headers,
                    json = params
                ) as response:
                    data = await response.json()
                    return data
                   
        except Exception as e:
            print(e)

