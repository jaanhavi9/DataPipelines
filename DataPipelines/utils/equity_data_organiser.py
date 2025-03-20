import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime


EQ_FOLDER = "EQ"  
OUTPUT_FOLDER = "partitioned_data" 


os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def read_and_combine_csv():

    all_data = []
    
    for file in os.listdir(EQ_FOLDER):
        if file.endswith(".csv"):
            file_path = os.path.join(EQ_FOLDER, file)
            column_names = ["Date", "Open", "High", "Low", "Close", "Volume", "OI"]
            df = pd.read_csv(file_path, names=column_names)

           
            if 'Date' not in df.columns:
                print(f"Skipping {file}, missing 'Date' column")
                continue

            df['Date'] = pd.to_datetime(df['Date'].astype(str), format="%Y%m%d", errors='coerce')

            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

            df['Date'] = pd.to_datetime(df['Date'])

            df['StockName'] = file.replace(".csv", "")  
            
            all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    
    return combined_df

def partition_and_save_parquet(df):
    """
    Partitions the DataFrame by Year → Month → Day and saves each day's data as a Parquet file.
    """
    df['Date'] = pd.to_datetime(df['Date'])  
    
    for date, group in df.groupby(df['Date'].dt.date):  
        year, month, day = date.year, date.month, date.day
        
        partition_folder = os.path.join(OUTPUT_FOLDER, f"{year}/{month:02d}")
        os.makedirs(partition_folder, exist_ok=True)
        
        file_path = os.path.join(partition_folder, f"{day:02d}.parquet")
        
        group['Date'] = pd.to_datetime(group['Date'])

        group.to_parquet(file_path, engine="fastparquet", index=False)
        print(f"Saved: {file_path}")

df_combined = read_and_combine_csv()
partition_and_save_parquet(df_combined())

print("Data partitioning completed!")
