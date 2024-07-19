import pandas as pd
import numpy as np
import os
from tqdm import tqdm  # Progress bar

def load_and_join(root_directory, subdir_track_playlist, subdir_track_artist, subdir_track, output_file, sample_fraction=0.1):
    # Define paths to the subdirectories
    path_track_playlist = os.path.join(root_directory, subdir_track_playlist)
    path_track_artist = os.path.join(root_directory, subdir_track_artist)
    path_track = os.path.join(root_directory, subdir_track)
    
    # Helper function to load and sample data
    def load_sample_data(path, desc):
        files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]
        sampled_files = np.random.choice(files, int(len(files) * sample_fraction), replace=False)  # Sample files without replacement
        data = pd.concat([pd.read_parquet(f) for f in tqdm(sampled_files, desc=f'Reading {desc}')], ignore_index=True)
        return data
    
    # Load and sample data from each directory
    data_track = load_sample_data(path_track, 'track')
    data_track_playlist = load_sample_data(path_track_playlist, 'track_playlist')
    data_track_artist = load_sample_data(path_track_artist, 'track_artist')
    
    # Perform the first inner join on 'id'
    joined_data = pd.merge(data_track, data_track_playlist, left_on='id', right_on='track_id', how='inner')

    # Perform the second inner join with the 'track_id' DataFrame
    final_joined_data = pd.merge(joined_data, data_track_artist, on='track_id', how='inner')

    # Save the resulting dataframe
    final_joined_data.to_csv(output_file, index=False)
    
    return final_joined_data

# Usage
root_directory = '/path/to/spark-warehouse'
output_file = 'final_join_output.csv'
result = load_and_join(root_directory, 'track_playlist1', 'track_artist1', 'track', output_file)

if result is not None and not result.empty:
    print(result.head())
else:
    print("No data returned from join operations.")
