from fastapi import FastAPI, Query, HTTPException
import boto3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import botocore.exceptions
import DataPipelines.brokers.dhan_broker as dhan_broker
from DataPipelines.utils.logger import setup_logger


app = FastAPI()

DATABASE = "historical_data_eq"  
TABLE = "eq"  
S3_OUTPUT = "s3://plus91testing/athena-query-results/" 

logger = setup_logger("dhanlogger", "dhan_logger.log")

broker = dhan_broker.DhanBroker(account_name="ACC1", logger=logger)

try:
    athena_client = boto3.client("athena")
except botocore.exceptions.NoCredentialsError:
    raise HTTPException(status_code=500, detail="AWS credentials not found. Ensure that AWS credentials are properly configured.")
except botocore.exceptions.EndpointConnectionError:
    raise HTTPException(status_code=500, detail="Unable to connect to AWS Athena. Check your internet connection and AWS service availability.")


def convert_date_to_epoch(date: str):
    """
    Converts a date string (YYYY-MM-DD) to nanoseconds epoch time.
    """
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        epoch_time = int(date_obj.timestamp() * 1_000_000_000)  
        return epoch_time
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date}. Use 'YYYY-MM-DD'.")


def resample_data(df, interval):
    """
    Resamples the given DataFrame based on the specified interval.
    """
    try:
        df["date"] = pd.to_numeric(df["date"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"] // 1_000_000, unit="ms")

        df.set_index("date", inplace=True)

        interval_mapping = {
            "1d": "D",
            "1w": "W",  
            "1mo": "M",  
            "3mo": "3M", 
            "6m": "6M", 
            "1y": "Y"   
        }

        if interval not in interval_mapping:
            raise HTTPException(status_code=400, detail="Invalid interval format. Use '1d', '1w', '1mo', '3mo', '6m', '1y'.")

        resampled_df = df.resample(interval_mapping[interval]).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna().reset_index()

        return resampled_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error while resampling data: {str(e)}")


def run_athena_query(query: str):
    """
    Executes an Athena query and returns the results as a DataFrame.
    """
    try:
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
            raise HTTPException(status_code=500, detail=f"Athena query failed: {error_message}")

        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = results["ResultSet"]["Rows"]

        if not rows:
            return pd.DataFrame() 


        columns = [col["VarCharValue"] for col in rows[0]["Data"]]
        data = [[col.get("VarCharValue", None) for col in row["Data"]] for row in rows[1:]]

        return pd.DataFrame(data, columns=columns)
    except botocore.exceptions.BotoCoreError as e:
        raise HTTPException(status_code=500, detail=f"AWS Athena error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error while querying Athena: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize the broker when the app starts."""
    await broker.initialize()


@app.get("/data/historical/single-day")
def get_data_single_day(security_id: str, date: str):
    """
    Retrieves data for a single day.
    Date format: YYYY-MM-DD
    Security ID format: NSE_EQ:1333
    """
    try:
        
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        epoch_time = convert_date_to_epoch(date)
        year = date_obj.year
        month = date_obj.month

        query = f"""
            SELECT * 
            FROM {DATABASE}.{TABLE} 
            WHERE partition_0 = '{year}' 
              AND partition_1 = '{month}' 
              AND security_id = '{security_id}' 
              AND date = {epoch_time};
        """
        df = run_athena_query(query)

        if df.empty:
            return {"message": "No data found for the given date."}

        return df.to_dict(orient="records")

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching single-day data: {str(e)}")



@app.get("/data/historical/date-range")
def get_data_date_range(security_id: str, start_date: str, end_date: str, interval: str = "1d"):
    """
    Retrieves data for a given date range with specified interval.
    - Date format: YYYY-MM-DD
    - Security ID format: NSE_EQ:1333
    - Interval options: '1d', '1w', '1mo', '3mo', '6m', '1y'
    """
    try:
        
        start_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_obj = datetime.strptime(end_date, "%Y-%m-%d")

        if start_obj > end_obj:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")

        start_epoch = convert_date_to_epoch(start_date)
        end_epoch = convert_date_to_epoch(end_date)

        query = f"""
            SELECT * 
            FROM {DATABASE}.{TABLE} 
            WHERE security_id = '{security_id}'
              AND date BETWEEN {start_epoch} AND {end_epoch};
        """
        df = run_athena_query(query)

        if df.empty:
            return {"message": "No data found for the given date range."}

        df_resampled = resample_data(df, interval)
        return df_resampled.to_dict(orient="records")

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching date-range data: {str(e)}")


@app.get("/data/live/ohlc-quote/{exchange_token}")
async def get_ohlc_quote(exchange_token: str):
    """Get OHLC data for a given exchange token."""
    try:
        data = await broker.ohlc_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching OHLC quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/ltp-quote/{exchange_token}")
async def get_ltp_quote(exchange_token: str):
    """Get the last traded price (LTP) for a given exchange token."""
    try:
        data = await broker.ltp_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching LTP quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/full-market-quote/{exchange_token}")
async def get_full_market_quote(exchange_token: str):
    """Get full market depth for a given exchange token."""
    try:
        data = await broker.full_market_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching full market quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/option-chain/{exchange_token}")
async def get_option_chain(
    exchange_token: str,
    expiry_date: Optional[str] = None,
):
    """Get option chain data for a given exchange token."""
    try:
        data = await broker.option_chain(
            exchange_token=exchange_token,
            expiry_date=expiry_date,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/live/expiry-dates/{exchange_token}")
async def get_expiry_dates(exchange_token: str):
    """Get expiry dates for a given exchange token."""
    try:
        data = await broker.expiry_dates(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching expiry dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
