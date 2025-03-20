import os
import re
import pandas as pd

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

ticker_pattern = re.compile(r'^(.*?)(\d{2})(?=JANFUT)(JANFUT)$')

month_str_to_num = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12
}

def update_expiry(row, expiry_mapping):
    """
    If Expiry is null, parse the Ticker to extract the two-digit year and month,
    convert to full year and month number, and update Expiry using the expiry_mapping.
    """
    if pd.notna(row.get("Expiry")):
        return row  

    ticker = row.get("Ticker")
    if not ticker:
        return row

    m = ticker_pattern.match(ticker)
    if m:
        two_digit_year = m.group(2)
        month_code = m.group(3)

        try:
            year_int = int(two_digit_year)
            full_year = 2000 + year_int if year_int < 100 else year_int
        except Exception as e:
            print(f"Error converting year for ticker {ticker}: {e}")
            return row

        month_int = month_str_to_num.get(month_code, None)
        if month_int is None:
            print(f"Warning: Unrecognized month code {month_code} in ticker {ticker}")
            return row

        expiry_date = expiry_mapping.get((full_year, month_int))
        if expiry_date:
            row["Expiry"] = expiry_date
        else:
            print(f"Warning: No expiry found for year {full_year}, month {month_int} for ticker {ticker}")
    else:
        print(f"Warning: Ticker {ticker} does not match expected format.")

    return row

def process_parquet_file(file_path, expiry_mapping):
    """
    Reads a Parquet file, updates rows with null Expiry values using the expiry mapping,
    and writes the updated DataFrame back to the same file.
    """
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return

    df = df.apply(lambda row: update_expiry(row, expiry_mapping), axis=1)

    null_expiry_count = df["Expiry"].isna().sum()
    if null_expiry_count > 0:
        print(f"Quality Check: {null_expiry_count} rows in file {file_path} still have null Expiry.")

    try:
        df.to_parquet(file_path, index=False)
        print(f"Updated file: {file_path}")
    except Exception as e:
        print(f"Error writing file {file_path}: {e}")
for year in os.listdir(PARTITIONED_DATA_PATH):
    year_path = os.path.join(PARTITIONED_DATA_PATH, year)
    if not os.path.isdir(year_path):
        continue
    for month in os.listdir(year_path):
        month_path = os.path.join(year_path, month)
        if not os.path.isdir(month_path):
            continue
        for file in os.listdir(month_path):
            if file.endswith(".parquet"):
                file_path = os.path.join(month_path, file)
                print(f"Processing file: {file_path}")
                process_parquet_file(file_path, expiry_mapping)

print("Expiry update for FUT files completed.")
