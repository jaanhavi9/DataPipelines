import os
import pandas as pd
import pyarrow.parquet as pq

PARTITIONED_DATA_PATH = "../historical_data/partitioned_data"
REFERENCE_FILE_PATH = "security_mapping.csv"

reference_df = pd.read_csv(REFERENCE_FILE_PATH)  

filtered_reference = reference_df[
    (reference_df["SEM_EXM_EXCH_ID"] == "NSE") &
    (reference_df["SEM_INSTRUMENT_NAME"] == "EQUITY")
]

stock_to_security_id = {
    row["SEM_TRADING_SYMBOL"]: f"NSE_EQ:{row['SEM_SMST_SECURITY_ID']}"
    for _, row in filtered_reference.iterrows()
}

def process_parquet_file(file_path):
    """
    Reads a Parquet file, updates `StockName` if necessary, adds `security_id`, and writes it back.
    """
    df = pd.read_parquet(file_path)

    df["StockName"] = df["StockName"].apply(lambda x: "HCL-INSYS" if x == "HCL_INSYS" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "BAJAJ-AUTO" if x == "BAJAJ_AUTO" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "S&SPOWER" if x == "S_SPOWER" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "M&M" if x == "M_M" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "M&MFIN" if x == "M_MFIN" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "ARE&M" if x == "ARE_M" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "J&KBANK" if x == "J_KBANK" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "IL&FSENGG" if x == "IL_FSENGG" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "MRO-TEK" if x == "MRO_TEK" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "GVT&D" if x == "GVT_D" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "NAM-INDIA" if x == "NAM_INDIA" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "IL&FSTRANS" if x == "IL_FSTRANS" else x)
    df["StockName"] = df["StockName"].apply(lambda x: "SURANAT&P" if x == "SURANAT_P" else x)

    df["security_id"] = df["StockName"].map(stock_to_security_id)

    df.to_parquet(file_path, index=False)
    print(f"Updated: {file_path}")

for year in os.listdir(PARTITIONED_DATA_PATH):
    year_path = os.path.join(PARTITIONED_DATA_PATH, year)
    if os.path.isdir(year_path):
        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if os.path.isdir(month_path):
                for file in os.listdir(month_path):
                    if file.endswith(".parquet"):
                        file_path = os.path.join(month_path, file)
                        process_parquet_file(file_path)

print("All Parquet files updated successfully.")
