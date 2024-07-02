import pandas as pd
import os
from tqdm import tqdm  # Import tqdm for the progress bar

def process_parquet_files_batched(root_directory, batch_size=10):
    # Collect all parquet file paths
    parquet_files = []
    for subdir, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(subdir, file)
                parquet_files.append(file_path)

    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()
    temp_data = []

    # Process each file with a tqdm progress bar
    for file_path in tqdm(parquet_files, desc="Processing Parquet Files"):
        try:
            df = pd.read_parquet(file_path)
            temp_data.append(df)
            
            # Concatenate and clear temp data when reaching batch size
            if len(temp_data) >= batch_size:
                all_data = pd.concat([all_data, pd.concat(temp_data, ignore_index=True)], ignore_index=True)
                temp_data = []  # Clear the temporary list after concatenation
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")

    # Process any remaining dataframes
    if temp_data:
        all_data = pd.concat([all_data, pd.concat(temp_data, ignore_index=True)], ignore_index=True)
    
    return all_data

# Usage
root_directory = '/home/user/big/spark-warehouse'
data = process_parquet_files_batched(root_directory)
print(data.head())

# Save the DataFrame to a file
data.to_parquet('/home/user/big/spark-warehouse/combined_data.parquet')
