import os
import re
import pandas as pd
import pyarrow.parquet as pq


PARTITIONED_DATA_PATH = "fut"
EXPIRY_CSV_PATH = "../historical_data/expiry.csv"

expiry_df = pd.read_csv(EXPIRY_CSV_PATH)
expiry_mapping = {}
for _, row in expiry_df.iterrows():
    try:
        full_year = int(row["Year"])
        month_num = int(row["Month"])
        expiry_date = row["Expiry"]
        expiry_mapping[(full_year, month_num)] = expiry_date
    except Exception as e:
        print(f"Error processing expiry row {row}: {e}")

ticker_pattern = re.compile(r'^(.*?)(\d{2})([A-Z]{3})FUT$')

month_str_to_num = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

def update_futures_row(row, file_year, file_month, expiry_mapping):
    """
    If Expiry is null and the Ticker matches the FUT pattern,
    update the ticker for next year's contracts (NOV & DEC files) and fill in the expiry date.
    """
    ticker = row.get("Ticker")
    expiry = row.get("Expiry")

    if ticker and pd.isna(expiry):
        m = ticker_pattern.match(ticker)
        if m:
            base = m.group(1)  # Stock name (e.g., ADANIPORTS)
            old_year = m.group(2)  # Two-digit year (e.g., 12 for 2012)
            month_code = m.group(3)  # Three-letter month (e.g., JAN, FEB)

            try:
                year_int = int(old_year)
                full_year = 2000 + year_int if year_int < 100 else year_int
            except Exception as e:
                print(f"Error converting year for ticker {ticker}: {e}")
                return row

            month_int = month_str_to_num.get(month_code, None)
            if month_int is None:
                print(f"Warning: Unrecognized month code {month_code} in ticker {ticker}")
                return row

            if file_month in [11, 12] and month_int in [1, 2, 3]:
                corrected_year = file_year + 1
                corrected_year_str = str(corrected_year)[-2:]  
                new_ticker = f"{base}{corrected_year_str}{month_code}FUT"
                row["Ticker"] = new_ticker
                full_year = corrected_year  

            expiry_date = expiry_mapping.get((full_year, month_int))
            if expiry_date:
                row["Expiry"] = expiry_date
            else:
                print(f"Warning: No expiry found for year {full_year}, month {month_int} for ticker {ticker}")

    return row

def process_parquet_file(file_path, file_year, file_month, expiry_mapping):
    """
    Reads a Parquet file, updates futures rows with missing expiry dates,
    and corrects tickers for NOV & DEC files if necessary.
    """
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return

    df = df.apply(lambda row: update_futures_row(row, file_year, file_month, expiry_mapping), axis=1)

    null_expiry_count = df["Expiry"].isna().sum()
    if null_expiry_count > 0:
        print(f"Quality Check: {null_expiry_count} rows in {file_path} still have null Expiry.")

    try:
        df.to_parquet(file_path, index=False)
        print(f"Updated file: {file_path}")
    except Exception as e:
        print(f"Error writing file {file_path}: {e}")

for year in os.listdir(PARTITIONED_DATA_PATH):
    year_path = os.path.join(PARTITIONED_DATA_PATH, year)
    if not os.path.isdir(year_path):
        continue
    try:
        file_year = int(year)
    except ValueError:
        print(f"Skipping non-year directory: {year}")
        continue

    for month in os.listdir(year_path):
        month_path = os.path.join(year_path, month)
        if not os.path.isdir(month_path):
            continue
        try:
            file_month = int(month)
        except ValueError:
            print(f"Skipping non-month directory: {month}")
            continue

        for file in os.listdir(month_path):
            if file.endswith(".parquet"):
                file_path = os.path.join(month_path, file)
                print(f"Processing file: {file_path}")
                process_parquet_file(file_path, file_year, file_month, expiry_mapping)

print("Futures ticker correction and expiry update completed.")
