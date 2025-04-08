from fastapi import FastAPI, Query, HTTPException
import boto3
import pandas as pd
from datetime import datetime
from typing import Optional
import DataPipelines.brokers.dhan_broker as dhan_broker
from DataPipelines.utils.logger import setup_logger
import os
from datetime import time

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "ap-south-1")

app = FastAPI()

logger = setup_logger("dhanlogger", "dhan_logger.log")

EQ_DATABASE = "historical_data_eq"
EQ_TABLE = "eq"
FUT_DATABASE = "historical_fno_data"
FUT_TABLE = "fut"
FUT_DATABASE_MIN = "historical_fno_data_1min"
OPT_DATABASE = "historical_fno_data"
OPT_TABLE = "opt"
OPT_DATABASE_MIN = "historical_fno_data_1min"
S3_OUTPUT = "s3://plus91testing/athena-query-results/"

master_scrip = pd.read_csv("DataPipelines/utils/plus91_master_scrip_eq.csv")

athena_client = boto3.client(
    "athena",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

print("Initializing Dhan Broker...")
broker = dhan_broker.DhanBroker(account_name="ACC1", logger=logger)


#function to convert nanoseconds to datetime - useful for displaying results
def nanoseconds_to_datetime(ns):
    seconds = ns / 1e9
   
    return datetime.utcfromtimestamp(seconds)


def convert_date_to_epoch(date: str):
    """Converts a date string (YYYY-MM-DD) to epoch time in nanoseconds."""
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        epoch_time = int(date_obj.timestamp() * 1_000_000_000)
        print(f"Converted date {date} to epoch time {epoch_time}")
        return epoch_time
    except ValueError:
        print(f"Invalid date format: {date}")
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date}. Use 'YYYY-MM-DD'.")

def resample_data(df, interval):
    """Resamples the given DataFrame based on the specified interval."""
    try:
        print(f"Resampling data with interval: {interval}")

        if 'datetime' in df.columns:
            df.rename(columns={'datetime': 'date'}, inplace=True)

        if pd.api.types.is_numeric_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"] // 1_000_000, unit="ms", errors="coerce")
        else:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.set_index("date", inplace=True)

        interval_mapping = {
            "1m": "min", "2m": "2min", "5m": "5min", "15m": "15min", "30m": "30min", "60m": "60min",
            "1min": "min", "2min": "2min", "5min": "5min", "15min": "15min", "30min": "30min", "60min": "60min",
            "1h": "H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H",
            "1d": "D", "1w": "W", "1mo": "M", "3mo": "3M", "6m": "6M", "1y": "Y"
        }
        if interval not in interval_mapping:
            raise HTTPException(status_code=400, detail="Invalid interval format.")
        resample_rule = interval_mapping[interval]

        groupby_cols = [col for col in ['strikeprice', 'option_type'] if col in df.columns]
        print("Using groupby columns:", groupby_cols)

        numeric_cols = ["open", "high", "low", "close", "volume"]
        if "open_interest" in df.columns:
            numeric_cols.append("open_interest")
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

        agg_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }
        if "open_interest" in df.columns:
            agg_dict["open_interest"] = "last"

        if groupby_cols:
            resampled_dfs = []
            for keys, group_df in df.groupby(groupby_cols):
              
                keys = (keys,) if not isinstance(keys, tuple) else keys
                resampled = group_df.resample(resample_rule).agg(agg_dict).dropna()
       
                for col, val in zip(groupby_cols, keys):
                    resampled[col] = val
                resampled_dfs.append(resampled)
            resampled_df = pd.concat(resampled_dfs).reset_index()
        else:
            resampled_df = df.resample(resample_rule).agg(agg_dict).dropna().reset_index()

        print("Successfully resampled data.")
        return resampled_df

    except Exception as e:
        print(f"Error while resampling data: {e}")
        raise HTTPException(status_code=500, detail=f"Error while resampling data: {e}")



#function to run athena query
def run_athena_query(query: str, DATABASE):
    """Executes an Athena query and returns the results as a DataFrame."""
    
    try:
        print(f"Executing Athena query: {query}")
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": S3_OUTPUT},
        )
        query_execution_id = response["QueryExecutionId"]

        while True:
            status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

        if state != "SUCCEEDED":
            error_message = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            print(f"Athena query failed: {error_message}")
            raise HTTPException(status_code=500, detail=f"Athena query failed: {error_message}")

        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = results["ResultSet"]["Rows"]

        if not rows:
            print("Athena query returned no data.")
            return pd.DataFrame()

        columns = [col["VarCharValue"] for col in rows[0]["Data"]]
        data = [[col.get("VarCharValue", None) for col in row["Data"]] for row in rows[1:]]

        df = pd.DataFrame(data, columns=columns)
        print(f"Athena query returned {len(df)} rows.")
        return df

    except Exception as e:
        print(f"Unexpected error while querying Athena: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error while querying Athena: {str(e)}")



#Function to initialise dhan account
@app.on_event("startup")
async def startup_event():
    """Initialize the broker when the app starts."""
    print("Initializing broker at startup...")
    await broker.initialize()


#Function to get historical expiry dates
@app.get("/data/historical/expiry")
def get_expiry(year: int, month: int):
    """ Get Expiry from 2010 - 2025/04 """
    print(f"Fetching expiry for {year} and {month}")
    try:
        expiry_df = pd.read_csv("expiry.csv")
        filtered_df = expiry_df[(expiry_df['Year'] == year) & (expiry_df['Month'] == month)]['Expiry'].values[0]
     
        return {"data": filtered_df}
    except Exception as e:
        print("Error in getting Expiry Date")
        print(e)


#Function to get security_id for a trading symbol
@app.get("/get_security_id")
def get_security_id(trading_symbol: str = Query(..., description="Trading symbol to lookup")):
    """
    Returns the security_id for a given trading symbol.
    
    Args:
        trading_symbol: The trading symbol to lookup (e.g., 'OMAXAUTO', 'ADSL')
    
    Returns:
        Dictionary containing the security_id if found, or error message if not found
    """
    print(f"Looking up security_id for trading symbol: {trading_symbol}")
    
    try:
   
        master_scrip = pd.read_csv("DataPipelines/utils/plus91_master_scrip_eq.csv")
        
       
        match = master_scrip[master_scrip['Trading Symbol'].str.upper() == trading_symbol.upper()]
        
        if not match.empty:
            security_id = match.iloc[0]['Security_id']
            print(f"Found security_id {security_id} for trading symbol {trading_symbol}")
            return {
                "status": "success",
                "trading_symbol": trading_symbol,
                "security_id": security_id
            }
        else:
            print(f"No security_id found for trading symbol: {trading_symbol}")
            raise HTTPException(
                status_code=404,
                detail=f"No security_id found for trading symbol: {trading_symbol}"
            )
            
    except Exception as e:
        print(f"Error looking up security_id: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error looking up security_id: {str(e)}"
        )
    

################################
'''

1d - Single Day

'''
################################


##########       EQ         ###########
@app.get("/data/historical/1d/eq/single-day")
def get_data_single_day(security_id: str, date: str):
    """Retrieves data for a single day."""
    print(f"Fetching data for {security_id} on {date}")
    try:
        epoch_time = convert_date_to_epoch(date)
        query = f"SELECT * FROM {EQ_DATABASE}.{EQ_TABLE} WHERE security_id = '{security_id}' AND date = {epoch_time};"
        df = run_athena_query(query, EQ_DATABASE)
       
        if df.empty:
            print(f"No data found for {security_id} on {date}")
            return {"message": "No data found for the given date or incorrect parameters given."}
        
        df['date'] = df['date'].astype(int).apply(nanoseconds_to_datetime)
        return df.to_dict(orient="records")
    
    except Exception as e:
        print(f"Error fetching single-day data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


##########      FUT         ##############
@app.get("/data/historical/1d/fut/single-day")
def get_data_single_day(security_id: str, date: str, expiry: int):
    """Retrieves data for a single day."""
    print(f"Fetching data for {security_id} on {date} with expiry index {expiry}")
    try:
        epoch_time = convert_date_to_epoch(date)

        query = f"SELECT * FROM {FUT_DATABASE}.{FUT_TABLE} WHERE security_id = '{security_id}' AND date = {epoch_time};"
        df = run_athena_query(query, FUT_DATABASE)

        if df.empty:
            print(f"No data found for {security_id} on {date}")
            return {"message": "No data found for the given date."}

       
        df['expiry'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime) 

        df = df.sort_values(by='expiry')

        unique_expiry_dates = df['expiry'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]

        filtered_df = df[df['expiry'] == selected_expiry]
        filtered_df['date'] = filtered_df['date'].astype(int).apply(nanoseconds_to_datetime)

        return filtered_df.to_dict(orient="records")

    except Exception as e:
        print(f"Error fetching single-day data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


############       OPT         ###############
@app.get("/data/historical/1d/opt/single-day")
def get_data_single_day(security_id: str, date: str,strike_price: float, option_type: str,  expiry: int):
    """Retrieves data for a single day."""
    print(f"Fetching data for {security_id} on {date} with expiry index {expiry}")
    try:
        epoch_time = convert_date_to_epoch(date)

        query = f"SELECT * FROM {OPT_DATABASE}.{OPT_TABLE} WHERE security_id = '{security_id}' AND datetime = {epoch_time}"

        if strike_price is not None:
            query += f" AND strikeprice = {float(strike_price)}"

        if option_type is not None:
            
            opt_type_upper = option_type.upper()
            if opt_type_upper in ['CALL', 'CE', 'Call']:
                query += f" AND (\"call/put\" = 'CE' OR \"call/put\" = 'Call')"
            elif opt_type_upper in ['PUT', 'PE', 'Put']:
                query += f" AND (\"call/put\" = 'PE' OR \"call/put\" = 'Put')"
        
        df = run_athena_query(query, OPT_DATABASE)

        if df.empty:
            print(f"No data found for {security_id} on {date}")
            return {"message": "No data found for the given date."}

       
        df['expiry'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime) 

        df = df.sort_values(by='expiry')

        unique_expiry_dates = df['expiry'].unique()

        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]

        filtered_df = df[df['expiry'] == selected_expiry]
        filtered_df['datetime'] = filtered_df['datetime'].astype(int).apply(nanoseconds_to_datetime)

        return filtered_df.to_dict(orient="records")

    except Exception as e:
        print(f"Error fetching single-day data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


##################################
'''

1 min - Single Day - Only for FnO

'''
##################################


############        FUT         ##############
@app.get("/data/historical/1min/fut/single-day")
def get_fut_1min_single_day(
    security_id: str, 
    date: str, 
    expiry: int,
    interval:str = "1m"
):
    """Retrieves complete 1-minute futures data for a single trading day with expiry selection."""
    print(f"Fetching full day 1min FUT data for {security_id} on {date} with expiry index {expiry}")

    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        start_of_day = int(date_obj.timestamp()) * 1_000_000_000
        end_of_day = start_of_day + (86400 * 1_000_000_000)  

        query = f"""
            SELECT * FROM {FUT_DATABASE_MIN}.{FUT_TABLE} 
            WHERE security_id = '{security_id}'
            AND datetime >= {start_of_day}
            AND datetime < {end_of_day}
        """
        
        query += " ORDER BY datetime ASC;"
        
        df = run_athena_query(query, FUT_DATABASE_MIN)

        if df.empty:
            print(f"No 1min FUT data found for {security_id} on {date}")
            return {"message": "No data found for the given date."}

     
        df['expiry_date'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime)
        df = df.sort_values(by='expiry_date')
        unique_expiry_dates = df['expiry_date'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]
        filtered_df = df[df['expiry_date'] == selected_expiry].copy()

 
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['datetime'].astype(int) // 1_000_000, unit='ms')
        
        if not filtered_df.empty:
            
            filtered_df.set_index('timestamp', inplace=True)
            
            if 'interval' not in filtered_df.columns or not all(filtered_df['interval'] == '1min'):
                filtered_df = filtered_df.resample('1T').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'open interest': 'last'
                }).dropna()
          
            market_open = time(9, 15)
            market_close = time(15, 30)
            full_range = pd.date_range(
                start=filtered_df.index.min().replace(hour=9, minute=15),
                end=filtered_df.index.max().replace(hour=15, minute=30),
                freq='1T'
            )
            filtered_df = filtered_df.reindex(full_range)
            
            filtered_df.ffill(inplace=True)

        filtered_df.reset_index(inplace=True)
        filtered_df.rename(columns={'index': 'datetime'}, inplace=True)
        filtered_df['datetime'] = filtered_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        filtered_df.drop(columns=['expiry_date'], inplace=True, errors='ignore')

        if interval != "1min" or interval != "1m":
            resampled_df = resample_data(filtered_df, interval)
            return resampled_df.to_dict(orient = "records")
        
        return filtered_df.to_dict(orient="records")

    except ValueError as ve:
        print(f"Invalid date format: {str(ve)}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    except Exception as e:
        print(f"Error fetching full day 1min FUT data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


############        OPTIONS         ##############
@app.get("/data/historical/1min/opt/single-day")
def get_options_1min_single_day(
    security_id: str,
    date: str,
    expiry: int,
    strike_price: Optional[float] = None,
    option_type: Optional[str] = None,
    interval: Optional[str] = "1m"
):
    """Retrieves complete 1-minute options data for a single trading day with expiry, strike price, and option type selection."""
    print(f"Fetching full day 1min OPTIONS data for {security_id} on {date} with expiry index {expiry}, strike {strike_price}, type {option_type}")

    try:
       
        if option_type and option_type.upper() not in ['CALL', 'PUT', 'CE', 'PE']:
            raise HTTPException(status_code=400, detail="Invalid option type. Must be 'CALL'/'CE' or 'PUT'/'PE'.")

        date_obj = datetime.strptime(date, "%Y-%m-%d")
        start_of_day = int(date_obj.timestamp()) * 1_000_000_000
        end_of_day = start_of_day + (86400 * 1_000_000_000)

        query = f"""
            SELECT 
                ticker,
                datetime,
                open,
                high,
                low,
                close,
                volume,
                "open interest" as open_interest,
                stockname,
                strikeprice,
                "call/put" as option_type,
                expiry,
                security_id
            FROM {OPT_DATABASE_MIN}.{OPT_TABLE} 
            WHERE security_id = '{security_id}'
            AND datetime >= {start_of_day}
            AND datetime < {end_of_day}
        """
    
        if strike_price is not None:
            query += f" AND strikeprice = {float(strike_price)}"

        if option_type is not None:
            
            opt_type_upper = option_type.upper()
            if opt_type_upper in ['CALL', 'CE']:
                query += f" AND (\"call/put\" = 'CE' OR \"call/put\" = 'Call')"
            elif opt_type_upper in ['PUT', 'PE']:
                query += f" AND (\"call/put\" = 'PE' OR \"call/put\" = 'Put')"
        
        query += " ORDER BY datetime ASC;"
        
        
        df = run_athena_query(query, OPT_DATABASE_MIN)    

        if df.empty:
            print(f"No 1min OPTIONS data found for {security_id} on {date}")
            return {"message": "No data found for the given criteria."}

        df['timestamp'] = pd.to_datetime(df['datetime'].astype(int) // 1_000_000, unit='ms')
        
        df['expiry_date'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime)
        df = df.sort_values(by='expiry_date')
        unique_expiry_dates = df['expiry_date'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]
        filtered_df = df[df['expiry_date'] == selected_expiry].copy()

        if not filtered_df.empty:
          
            filtered_df.set_index('timestamp', inplace=True)
            
            resample_columns = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'open_interest': 'last',
                'strikeprice': 'last',
                'option_type': 'last',
                'ticker': 'last',
                'stockname': 'last'
            }
            
            filtered_df = filtered_df.resample('1T').agg(resample_columns).dropna()
          
    
            market_open = time(9, 15)
            market_close = time(15, 30)
            full_range = pd.date_range(
                start=filtered_df.index.min().replace(hour=9, minute=15),
                end=filtered_df.index.max().replace(hour=15, minute=30),
                freq='1T'
            )
            filtered_df = filtered_df.reindex(full_range)
            
            ohlc_cols = ['open', 'high', 'low', 'close']
            last_cols = ['open_interest', 'strikeprice', 'option_type', 'ticker', 'stockname']
            
            filtered_df[ohlc_cols] = filtered_df[ohlc_cols].ffill()
            filtered_df[last_cols] = filtered_df[last_cols].fillna(method='ffill')
       
            if 'volume' in filtered_df.columns:
                filtered_df['volume'] = filtered_df['volume'].fillna(0)

        filtered_df.reset_index(inplace=True)
        filtered_df.rename(columns={'index': 'datetime'}, inplace=True)
        filtered_df['datetime'] = filtered_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        numeric_cols = ['open', 'high', 'low', 'close', 'strikeprice']
        for col in numeric_cols:
            if col in filtered_df.columns:
                filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')
        
        filtered_df.drop(columns=['expiry_date'], inplace=True, errors='ignore')

        if interval != "1min" or interval != "1m":
            resampled_df = resample_data(filtered_df, interval)
            return resampled_df.to_dict(orient = "records")
        
        return filtered_df.to_dict(orient="records")

    except ValueError as ve:
        print(f"Invalid date format: {str(ve)}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    except Exception as e:
        print(f"Error fetching full day 1min OPTIONS data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


################################################################################
'''

DATE RANGE ENDPOINTS - 1D

'''
################################################################################


#################         EQ          ####################
@app.get("/data/historical/1d/eq/date-range")
def get_data_date_range(security_id: str, start_date: str, end_date: str, interval: str = "1d"):
    """Retrieves historical data for a given date range and interval."""
    print(f"Fetching data for {security_id} from {start_date} to {end_date} with interval {interval}")
    try:
        start_epoch = convert_date_to_epoch(start_date)
        end_epoch = convert_date_to_epoch(end_date)

        if start_epoch > end_epoch:
            print("Start date cannot be after end date.")
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")

        query = f"""
            SELECT * FROM {EQ_DATABASE}.{EQ_TABLE} 
            WHERE security_id = '{security_id}' 
            AND date BETWEEN {start_epoch} AND {end_epoch};
        """
        df = run_athena_query(query, EQ_DATABASE)

        if df.empty:
            print(f"No data found for {security_id} from {start_date} to {end_date}")
            return {"message": "No data found for the given date range."}

        if interval != "1d":
            df_resampled = resample_data(df, interval)
            return df_resampled.to_dict(orient="records")
        
        return df.to_dict(orient="records")

    except Exception as e:
        print(f"Error fetching date-range data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


##############          FUT             #####################
@app.get("/data/historical/1d/fut/date-range")
def get_data_date_range(security_id: str, start_date: str, end_date: str, expiry : int, interval: str = "1d"):
    """Retrieves historical data for a given date range and interval."""
    print(f"Fetching data for {security_id} from {start_date} to {end_date} with interval {interval}")
    try:
        start_epoch = convert_date_to_epoch(start_date)
        end_epoch = convert_date_to_epoch(end_date)

        if start_epoch > end_epoch:
            print("Start date cannot be after end date.")
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")

        query = f"""
            SELECT * FROM {FUT_DATABASE}.{FUT_TABLE} 
            WHERE security_id = '{security_id}' 
            AND date BETWEEN {start_epoch} AND {end_epoch};
        """
        df = run_athena_query(query, FUT_DATABASE)
        
        if df.empty:
            print(f"No data found for {security_id} between {start_date} and {end_date}")
            return {"message": "No data found for the given date."}

       
        df['expiry'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime) 

        df = df.sort_values(by='expiry')

        unique_expiry_dates = df['expiry'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]

        filtered_df = df[df['expiry'] == selected_expiry]
        filtered_df['date'] = filtered_df['date'].astype(int).apply(nanoseconds_to_datetime)

        if interval != "1d":
            resampled_df = resample_data(filtered_df, interval)
            return resampled_df.to_dict(orient = "records")

        return filtered_df.to_dict(orient="records")

    except Exception as e:
        print(f"Error fetching date-range data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



#################         OPT          ####################
@app.get("/data/historical/1d/opt/date-range")
def get_options_date_range(
    security_id: str, 
    start_date: str, 
    end_date: str, 
    expiry: int,
    strike_price: Optional[float] = None,
    option_type: Optional[str] = None,
    interval: str = "1d"
):
    """Retrieves historical options data for a given date range with expiry, strike and type selection."""
    print(f"Fetching OPTIONS data for {security_id} from {start_date} to {end_date} with expiry {expiry}, strike {strike_price}, type {option_type}")
    
    try:
    
        if option_type and option_type.upper() not in ['CALL', 'PUT', 'CE', 'PE']:
            raise HTTPException(status_code=400, detail="Invalid option type. Must be 'CALL'/'CE' or 'PUT'/'PE'.")

        start_epoch = convert_date_to_epoch(start_date)
        end_epoch = convert_date_to_epoch(end_date)

        if start_epoch > end_epoch:
            print("Start date cannot be after end date.")
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")

       
        query = f"""
            SELECT * FROM {OPT_DATABASE}.{OPT_TABLE} 
            WHERE security_id = '{security_id}'
            AND datetime BETWEEN {start_epoch} AND {end_epoch}
        """

        if strike_price is not None:
            query += f" AND strikeprice = {float(strike_price)}"

        if option_type is not None:
            opt_type_upper = option_type.upper()
            if opt_type_upper in ['CALL', 'CE', 'Call']:
                query += f" AND (\"call/put\" = 'CE' OR \"call/put\" = 'Call')"
            elif opt_type_upper in ['PUT', 'PE', 'Put']:
                query += f" AND (\"call/put\" = 'PE' OR \"call/put\" = 'Put')"

        query += ";"
        
        df = run_athena_query(query, OPT_DATABASE)

        if df.empty:
            print(f"No OPTIONS data found for {security_id} between {start_date} and {end_date}")
            return {"message": "No data found for the given criteria."}

        df['expiry_date'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime)
        df = df.sort_values(by='expiry_date')
        unique_expiry_dates = df['expiry_date'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]
        filtered_df = df[df['expiry_date'] == selected_expiry].copy()

        if interval != "1d":
            filtered_df = resample_data(filtered_df, interval)
            return filtered_df.to_dict(orient = "records")

        filtered_df['datetime'] = pd.to_datetime(filtered_df['datetime'].astype(int) // 1_000_000, unit='ms')
        
        filtered_df.drop(columns=['expiry_date'], inplace=True, errors='ignore')
        
        return filtered_df.to_dict(orient="records")

    except Exception as e:
        print(f"Error fetching OPTIONS date-range data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    

############################################
'''                                        

DATE RANGE ENDPOINTS - 1MIN - ONLY FOR FNO

'''
############################################


##################          FUT         ####################
@app.get("/data/historical/1min/fut/date-range")
def get_fut_1min_date_range(
    security_id: str,
    start_date: str,
    end_date: str,
    expiry: int,
    interval: str = "1m"
):
    """Retrieves complete 1-minute futures data for a date range with expiry selection."""
    print(f"Fetching 1min FUT data for {security_id} from {start_date} to {end_date} with expiry index {expiry}")

    try:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        start_ns = int(start_date_obj.timestamp()) * 1_000_000_000
        end_ns = int(end_date_obj.timestamp()) * 1_000_000_000 + (86400 * 1_000_000_000)  

        if start_ns > end_ns:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date")

        query = f"""
            SELECT * FROM {FUT_DATABASE_MIN}.{FUT_TABLE} 
            WHERE security_id = '{security_id}'
            AND datetime >= {start_ns}
            AND datetime < {end_ns}
        """
        
        
        query += " ORDER BY datetime ASC;"
        
        df = run_athena_query(query, FUT_DATABASE_MIN)

        if df.empty:
            print(f"No 1min FUT data found for {security_id} between {start_date} and {end_date}")
            return {"message": "No data found for the given date range."}

        df['expiry_date'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime)
        df = df.sort_values(by='expiry_date')
        unique_expiry_dates = df['expiry_date'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]
        filtered_df = df[df['expiry_date'] == selected_expiry].copy()

        filtered_df['timestamp'] = pd.to_datetime(filtered_df['datetime'].astype(int) // 1_000_000, unit='ms')
        
        results = []
        for date, day_df in filtered_df.groupby(filtered_df['timestamp'].dt.date):
            day_df = day_df.copy()
            
            day_df.set_index('timestamp', inplace=True)
            
            if 'interval' not in day_df.columns or not all(day_df['interval'] == '1min'):
                day_df = day_df.resample('1T').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'open interest': 'last'
                }).dropna()
            
            market_open = time(9, 15)
            market_close = time(15, 30)
            full_range = pd.date_range(
                start=day_df.index.min().replace(hour=9, minute=15),
                end=day_df.index.max().replace(hour=15, minute=30),
                freq='1T'
            )
            day_df = day_df.reindex(full_range)
            
            day_df.ffill(inplace=True)
            
            day_df.reset_index(inplace=True)
            day_df.rename(columns={'index': 'datetime'}, inplace=True)
            day_df['datetime'] = day_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

            if interval != "1m":
                resampled_df = resample_data(day_df, interval)
                results.extend(resampled_df.to_dict(orient="records"))
                return results
            
            results.extend(day_df.to_dict(orient='records'))

        return results

    except ValueError as ve:
        print(f"Invalid date format: {str(ve)}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    except Exception as e:
        print(f"Error fetching date range 1min FUT data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


##################          OPTIONS         ####################
@app.get("/data/historical/1min/opt/date-range")
def get_options_1min_date_range(
    security_id: str,
    start_date: str,
    end_date: str,
    expiry: int,
    strike_price: Optional[float] = None,
    option_type: Optional[str] = None,
    interval: str = "1m"
):
    """Retrieves complete 1-minute options data for a date range with expiry, strike and type selection."""
    print(f"Fetching 1min OPTIONS data for {security_id} from {start_date} to {end_date} with expiry {expiry}, strike {strike_price}, type {option_type}")

    try:
      
        if option_type and option_type.upper() not in ['CALL', 'PUT', 'CE', 'PE']:
            raise HTTPException(status_code=400, detail="Invalid option type. Must be 'CALL'/'CE' or 'PUT'/'PE'.")

        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        start_ns = convert_date_to_epoch(start_date)
        end_ns = convert_date_to_epoch(end_date)  

        if start_ns > end_ns:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date")

    
        query = f"""
            SELECT 
                ticker,
                datetime,
                open,
                high,
                low,
                close,
                volume,
                "open interest" as open_interest,
                stockname,
                strikeprice,
                "call/put" as option_type,
                expiry,
                security_id
            FROM {OPT_DATABASE_MIN}.{OPT_TABLE} 
            WHERE security_id = '{security_id}'
            AND datetime >= {start_ns}
            AND datetime < {end_ns}
        """

        if strike_price is not None:
            query += f" AND strikeprice = {float(strike_price)}"

        if option_type is not None:
            opt_type_upper = option_type.upper()
            if opt_type_upper in ['CALL', 'CE']:
                query += f" AND (\"call/put\" = 'CE' OR \"call/put\" = 'Call')"
            elif opt_type_upper in ['PUT', 'PE']:
                query += f" AND (\"call/put\" = 'PE' OR \"call/put\" = 'Put')"
        
        query += " ORDER BY datetime ASC;"
        
        df = run_athena_query(query, OPT_DATABASE_MIN)

        if df.empty:
            print(f"No 1min OPTIONS data found for {security_id} between {start_date} and {end_date}")
            return {"message": "No data found for the given criteria."}

        df['expiry_date'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime)
        df = df.sort_values(by='expiry_date')
        unique_expiry_dates = df['expiry_date'].unique()
    
        if expiry >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}

        selected_expiry = unique_expiry_dates[expiry]
        filtered_df = df[df['expiry_date'] == selected_expiry].copy()

        filtered_df['timestamp'] = pd.to_datetime(filtered_df['datetime'].astype(int) // 1_000_000, unit='ms')
            
        if interval != "1m":
            resampled_df = resample_data(filtered_df, interval)
         
            return resampled_df.to_dict(orient="records")
        return filtered_df.to_dict(orient="records")

  

    except ValueError as ve:
        print(f"Invalid date format: {str(ve)}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    except Exception as e:
        print(f"Error fetching date range 1min OPTIONS data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


################################################
'''

OPTION CHAIN - 1day

'''
################################################


@app.get("/data/historical/1d/opt/option-chain")
def get_option_chain_1d(
    security_id: str,
    date: str,
    expiry_index: Optional[int] = Query(0, description="Index of expiry to select (0=nearest, 1=next, etc.)"),
    min_strike: Optional[float] = Query(None, description="Minimum strike price to include"),
    max_strike: Optional[float] = Query(None, description="Maximum strike price to include"),
    strike_step: Optional[float] = Query(None, description="Step between strike prices (e.g., 50 for 50-point intervals)")
):
    """
    Retrieves complete option chain for a given date with strike price filtering for a specific expiry.
    Handles cases where either call or put data might be missing for certain strike prices.
    """
    print(f"Fetching 1d option chain for {security_id} on {date} with expiry index {expiry_index}")
    
    try:
        epoch_time = convert_date_to_epoch(date)
        

        query = f"""
            SELECT 
                ticker,
                datetime,
                open,
                high,
                low,
                close,
                volume,
                "open interest" as open_interest,
                stockname,
                strikeprice,
                "call/put" as option_type,
                expiry,
                security_id
            FROM {OPT_DATABASE}.{OPT_TABLE} 
            WHERE security_id = '{security_id}'
            AND datetime = {epoch_time}
            ORDER BY strikeprice, option_type;
        """
        
        df = run_athena_query(query, OPT_DATABASE)
        
        if df.empty:
            print(f"No option data found for {security_id} on {date}")
            return {"message": "No option data found for the given date."}
        

        df['expiry_date'] = df['expiry'].astype(int).apply(nanoseconds_to_datetime)
        df = df.sort_values(by='expiry_date')
        unique_expiry_dates = df['expiry_date'].unique()
        
        if expiry_index >= len(unique_expiry_dates):
            print(f"Invalid expiry index {expiry_index}. Only {len(unique_expiry_dates)} expiry dates found.")
            return {"message": f"Invalid expiry index. Only {len(unique_expiry_dates)} expiry dates available."}
        

        selected_expiry = unique_expiry_dates[expiry_index]
        filtered_df = df[df['expiry_date'] == selected_expiry].copy()
        
        if filtered_df.empty:
            print(f"No option data found for {security_id} on {date} with expiry {selected_expiry}")
            return {"message": f"No option data found for the selected expiry {selected_expiry}."}
        

        if min_strike is not None:
            filtered_df = filtered_df[filtered_df['strikeprice'] >= float(min_strike)]
        
        if max_strike is not None:
            filtered_df = filtered_df[filtered_df['strikeprice'] <= float(max_strike)]
        
        if strike_step is not None:
            strike_step = float(strike_step)
            unique_strikes = sorted(filtered_df['strikeprice'].unique())
            filtered_strikes = [strike for strike in unique_strikes 
                              if strike % strike_step == 0 or strike == unique_strikes[0] or strike == unique_strikes[-1]]
            filtered_df = filtered_df[filtered_df['strikeprice'].isin(filtered_strikes)]
        
        filtered_df['datetime'] = pd.to_datetime(filtered_df['datetime'].astype(int) // 1_000_000, unit='ms')
        
        all_strikes = sorted(filtered_df['strikeprice'].unique())
        
        call_data = []
        put_data = []
        
        calls = filtered_df[filtered_df['option_type'].str.upper().isin(['CALL', 'CE', 'Call'])]
        puts = filtered_df[filtered_df['option_type'].str.upper().isin(['PUT', 'PE', 'Put'])]
        
        call_dict = {row['strikeprice']: row for _, row in calls.iterrows()}
        put_dict = {row['strikeprice']: row for _, row in puts.iterrows()}
        
        option_chain = []
        for strike in all_strikes:
            strike_data = {
                'strike': strike,
                'expiry_date': selected_expiry.strftime('%Y-%m-%d')
            }
            

            if strike in call_dict:
                call_row = call_dict[strike]
                strike_data.update({
                    'call_open': call_row['open'],
                    'call_high': call_row['high'],
                    'call_low': call_row['low'],
                    'call_close': call_row['close'],
                    'call_volume': call_row['volume'],
                    'call_oi': call_row['open_interest']
                })
            
            else:
                strike_data.update({
                    'call_open': None,
                    'call_high': None,
                    'call_low': None,
                    'call_close': None,
                    'call_volume': None,
                    'call_oi': None
                })
            

            if strike in put_dict:
                put_row = put_dict[strike]
                strike_data.update({
                    'put_open': put_row['open'],
                    'put_high': put_row['high'],
                    'put_low': put_row['low'],
                    'put_close': put_row['close'],
                    'put_volume': put_row['volume'],
                    'put_oi': put_row['open_interest']
                })
            else:
                
                strike_data.update({
                    'put_open': None,
                    'put_high': None,
                    'put_low': None,
                    'put_close': None,
                    'put_volume': None,
                    'put_oi': None
                })
            
            option_chain.append(strike_data)
        
        
        option_chain = sorted(option_chain, key=lambda x: x['strike'])
        
        return option_chain
    
    except Exception as e:
        print(f"Error fetching 1d option chain: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    

################################################
'''

LIVE DATA ENDPOINTS - DHAN

'''
#################################################


@app.get("/data/live/ohlc-quote/{exchange_token}")
async def get_ohlc_quote(exchange_token: str):
    """Get OHLC data for a given exchange token."""
    print(f"Fetching OHLC data for {exchange_token}")
    try:
        data = await broker.ohlc_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Error fetching OHLC quote: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/data/live/ltp-quote/{exchange_token}")
async def get_ltp_quote(exchange_token: str):
    """Get the last traded price (LTP) for a given exchange token."""
    print(f"Fetching LTP for {exchange_token}")
    try:
        data = await broker.ltp_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Error fetching LTP quote: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/data/live/full-market-quote/{exchange_token}")
async def get_full_market_quote(exchange_token: str):
    """Get full market depth for a given exchange token."""
    print(f"Fetching full market depth for {exchange_token}")
    try:
        data = await broker.full_market_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Error fetching full market quote: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/data/live/option-chain/{exchange_token}")
async def get_option_chain(
    exchange_token: str,
    expiry_date: Optional[str] = None,
):
    """Get option chain data for a given exchange token."""
    print(f"Fetching option chain for {exchange_token} with expiry {expiry_date}")
    try:
        data = await broker.option_chain(
            exchange_token=exchange_token,
            expiry_date=expiry_date,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Error fetching option chain: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/data/live/expiry-dates/{exchange_token}")
async def get_expiry_dates(exchange_token: str):
    """Get expiry dates for a given exchange token."""
    print(f"Fetching expiry dates for {exchange_token}")
    try:
        data = await broker.expiry_dates(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Error fetching expiry dates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/live/historical-data/{exchange_token}/{start_date}/{end_date}")
async def get_historical_data(
    exchange_token: str,
    start_date: str,
    end_date: str,
):
    """Get historical data for a given exchange token."""
    print(f"Fetching historical data for {exchange_token} from {start_date} to {end_date}")
    try:
        data = await broker.historical_data(
            exchange_token=exchange_token,
            start_date=start_date,
            end_date=end_date,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Error fetching historical data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
####################################################################################################################
#                                                    MAIN METHOD                                                   #
####################################################################################################################

if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
