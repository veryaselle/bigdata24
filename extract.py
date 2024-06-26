import os
import pandas as pd

def process_parquet_files(root_directory):
    # DataFrame to save data
    all_data = pd.DataFrame()

    # sub dir
    for subdir, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(subdir, file)
                try:
                    
                    df = pd.read_parquet(file_path)
                    all_data = pd.concat([all_data, df], ignore_index=True)
                except Exception as e:
                    print(f"Error by reading {file_path}: {str(e)}")

    return all_data

root_directory = 'c:/Users/verya/Desktop/spark-warehouse'
data = process_parquet_files(root_directory)

print(data.head())
