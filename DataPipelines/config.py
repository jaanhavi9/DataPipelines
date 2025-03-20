import os
from dotenv import load_dotenv


load_dotenv()


SUPPORTED_BROKERS = ['Zerodha', 'Upstox','Dhan']

ZERODHA_ACCOUNTS = {
    'PMZ736' : {
        'API_KEY' : os.getenv('ZERODHA_PMZ736_API_KEY'),
        'API_SECRET' : os.getenv('ZERODHA_PMZ736_API_SECRET'),
        'PASSWD' : os.getenv('ZERODHA_PMZ736_PASSWORD'),
        'USERID' : os.getenv('ZERODHA_PMZ736_USERID'),
        'TOTP_KEY' : os.getenv('ZERODHA_PMZ736_TOTP_KEY')
    }
    # Add more accounts
}

UPSTOX_ACCOUNTS = {
    'GC3525' : {
        'API_KEY' : os.getenv('UPSTOX_GC3525_API_KEY'),
        'API_SECRET' : os.getenv('UPSTOX_GC3525_API_SECRET'),
        'PHONE_NO' : os.getenv('UPSTOX_GC3525_PHONE_NO'),
        'PIN_CODE' : os.getenv('UPSTOX_GC3525_CODE_6'),
        'REDIRECT_URL' : os.getenv('UPSTOX_GC3525_REDIRECT_URL'),
        'TOTP_KEY' : os.getenv('UPSTOX_GC3525_TOTP_KEY')
    },
    '2HCB67' : {
        'API_KEY' : os.getenv('UPSTOX_2HCB67_API_KEY'),
        'API_SECRET' : os.getenv('UPSTOX_2HCB67_API_SECRET'),
        'PHONE_NO' : os.getenv('UPSTOX_2HCB67_PHONE_NO'),
        'PIN_CODE' : os.getenv('UPSTOX_2HCB67_CODE_6'),
        'REDIRECT_URL' : os.getenv('UPSTOX_2HCB67_REDIRECT_URL'),
        'TOTP_KEY' : os.getenv('UPSTOX_2HCB67_TOTP_KEY')
    }
}

DHAN_ACCOUNTS = {
    'ACC1' :{
        "CLIENT_ID":os.getenv("DHAN_ACC1_CLIENT_ID"),
        "ACCESS_TOKEN":os.getenv("DHAN_ACC1_ACCESS_TOKEN")
    }
}

STRATEGY_CONFIGS = {
    'MomentumStrategyV1': {
        'PORTFOLIO_SIZE': 3,
        'STOP_LOSS': 0.1,
        'HOLDING_PERIOD': 6,
        'SORT_BY': 'marketcap',
        'SORT_ASCENDING': True ,
    }, 
    'EdosStrategy': {
        'PORTFOLIO_SIZE': 15
    }
}

