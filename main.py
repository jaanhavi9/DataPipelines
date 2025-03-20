from fastapi import FastAPI, Query, HTTPException
import boto3
import pandas as pd
from datetime import datetime
from typing import Optional
import DataPipelines.brokers.dhan_broker as dhan_broker
from DataPipelines.utils.logger import setup_logger

app = FastAPI()


logger = setup_logger("dhanlogger", "dhan_logger.log")

DATABASE = "historical_data_eq"
TABLE = "eq"
S3_OUTPUT = "s3://plus91testing/athena-query-results/"

athena_client = boto3.client("athena")

logger.info("Initializing Dhan Broker...")
broker = dhan_broker.DhanBroker(account_name="ACC1", logger=logger)


def convert_date_to_epoch(date: str):
    """Converts a date string (YYYY-MM-DD) to epoch time in nanoseconds."""
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        epoch_time = int(date_obj.timestamp() * 1_000_000_000)
        logger.info(f"Converted date {date} to epoch time {epoch_time}")
        return epoch_time
    except ValueError:
        logger.error(f"Invalid date format: {date}")
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date}. Use 'YYYY-MM-DD'.")

def resample_data(df, interval):
    """Resamples the given DataFrame based on the specified interval."""
    try:
        logger.info(f"Resampling data with interval: {interval}")
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
            logger.error(f"Invalid interval: {interval}")
            raise HTTPException(status_code=400, detail="Invalid interval format. Use '1d', '1w', '1mo', '3mo', '6m', '1y'.")

        resampled_df = df.resample(interval_mapping[interval]).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna().reset_index()

        logger.info("Successfully resampled data.")
        return resampled_df
    except Exception as e:
        logger.error(f"Error while resampling data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error while resampling data: {str(e)}")

def run_athena_query(query: str):
    """Executes an Athena query and returns the results as a DataFrame."""
    try:
        logger.info(f"Executing Athena query: {query}")
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
            logger.error(f"Athena query failed: {error_message}")
            raise HTTPException(status_code=500, detail=f"Athena query failed: {error_message}")

        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = results["ResultSet"]["Rows"]

        if not rows:
            logger.warning("Athena query returned no data.")
            return pd.DataFrame()

        columns = [col["VarCharValue"] for col in rows[0]["Data"]]
        data = [[col.get("VarCharValue", None) for col in row["Data"]] for row in rows[1:]]

        df = pd.DataFrame(data, columns=columns)
        logger.info(f"Athena query returned {len(df)} rows.")
        return df

    except Exception as e:
        logger.error(f"Unexpected error while querying Athena: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error while querying Athena: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize the broker when the app starts."""
    logger.info("Initializing broker at startup...")
    await broker.initialize()

@app.get("/data/historical/single-day")
def get_data_single_day(security_id: str, date: str):
    """Retrieves data for a single day."""
    logger.info(f"Fetching data for {security_id} on {date}")
    try:
        epoch_time = convert_date_to_epoch(date)
        query = f"SELECT * FROM {DATABASE}.{TABLE} WHERE security_id = '{security_id}' AND date = {epoch_time};"
        df = run_athena_query(query)
        if df.empty:
            logger.warning(f"No data found for {security_id} on {date}")
            return {"message": "No data found for the given date."}
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching single-day data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/data/historical/date-range")
def get_data_date_range(security_id: str, start_date: str, end_date: str, interval: str = "1d"):
    """Retrieves historical data for a given date range and interval."""
    logger.info(f"Fetching data for {security_id} from {start_date} to {end_date} with interval {interval}")
    try:
        start_epoch = convert_date_to_epoch(start_date)
        end_epoch = convert_date_to_epoch(end_date)

        if start_epoch > end_epoch:
            logger.error("Start date cannot be after end date.")
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")

        query = f"""
            SELECT * FROM {DATABASE}.{TABLE} 
            WHERE security_id = '{security_id}' 
            AND date BETWEEN {start_epoch} AND {end_epoch};
        """
        df = run_athena_query(query)

        if df.empty:
            logger.warning(f"No data found for {security_id} from {start_date} to {end_date}")
            return {"message": "No data found for the given date range."}

        df_resampled = resample_data(df, interval)
        return df_resampled.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching date-range data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/live/ohlc-quote/{exchange_token}")
async def get_ohlc_quote(exchange_token: str):
    """Get OHLC data for a given exchange token."""
    logger.info(f"Fetching OHLC data for {exchange_token}")
    try:
        data = await broker.ohlc_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching OHLC quote: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/live/ltp-quote/{exchange_token}")
async def get_ltp_quote(exchange_token: str):
    """Get the last traded price (LTP) for a given exchange token."""
    logger.info(f"Fetching LTP for {exchange_token}")
    try:
        data = await broker.ltp_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching LTP quote: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/data/live/full-market-quote/{exchange_token}")
async def get_full_market_quote(exchange_token: str):
    """Get full market depth for a given exchange token."""
    logger.info(f"Fetching full market depth for {exchange_token}")
    try:
        data = await broker.full_market_quote(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching full market quote: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/live/option-chain/{exchange_token}")
async def get_option_chain(
    exchange_token: str,
    expiry_date: Optional[str] = None,
):
    """Get option chain data for a given exchange token."""
    logger.info(f"Fetching option chain for {exchange_token} with expiry {expiry_date}")
    try:
        data = await broker.option_chain(
            exchange_token=exchange_token,
            expiry_date=expiry_date,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching option chain: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/live/expiry-dates/{exchange_token}")
async def get_expiry_dates(exchange_token: str):
    """Get expiry dates for a given exchange token."""
    logger.info(f"Fetching expiry dates for {exchange_token}")
    try:
        data = await broker.expiry_dates(exchange_token=exchange_token)
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Error fetching expiry dates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
