import os
import pandas as pd
import pyarrow.parquet as pq
import hashlib


PARTITIONED_DATA_PATH = "../historical_data/partitioned_data"
OUTPUT_MASTER_FILE = "master_scrip_eq.csv"


stock_data = {}


def parse_security_id(security_id):
    """
    Extract Exchange, Segment, and Exchange Token from security_id.
    Format: EXCHANGE_SEGMENT:EXCHANGETOKEN (e.g., NSE_EQ:1333)
    """
    try:
        if not security_id or pd.isna(security_id):  
            return None, None, None

        exchange_segment, exchange_token = security_id.split(":")
        exchange, segment = exchange_segment.split("_")
        return exchange, segment, exchange_token
    
    except ValueError:
        return None, None, None  


def process_parquet_file(file_path):
    """
    Reads a Parquet file and updates the stock_data dictionary.
    """
    df = pd.read_parquet(file_path)

    for _, row in df.iterrows():
        stock_name = row["StockName"]
        security_id = row["security_id"]

        if stock_name not in stock_data:
            exchange, segment, exchange_token = parse_security_id(security_id)

            if exchange and segment and exchange_token:
                stock_data[stock_name] = {
                    "Plus91_id": hashlib.sha256(stock_name.encode()).hexdigest(),
                    "Trading Symbol": stock_name,
                    "Security_id": security_id,
                    "Exchange": exchange,
                    "Segment": segment,
                    "Exchange Token": exchange_token,
                }


for year in os.listdir(PARTITIONED_DATA_PATH):
    year_path = os.path.join(PARTITIONED_DATA_PATH, year)
    if os.path.isdir(year_path):
        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if os.path.isdir(month_path):
                for file in os.listdir(month_path):
                    if file.endswith(".parquet"):
                        file_path = os.path.join(month_path, file)
                        print("Processing file - ",file_path)
                        process_parquet_file(file_path)


master_df = pd.DataFrame.from_dict(stock_data, orient="index")


master_df.to_csv(OUTPUT_MASTER_FILE, index=False)
print(f"Master scrip file saved as {OUTPUT_MASTER_FILE}")
