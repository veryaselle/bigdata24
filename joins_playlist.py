import pandas as pd
import numpy as np
import os
from tqdm import tqdm  # Progress bar

def load_and_join_playlist(root_directory, subdir_track_playlist, subdir_playlist, output_file, sample_fraction=0.1):
    # Define paths to the subdirectories
    path_track_playlist = os.path.join(root_directory, subdir_track_playlist)
    path_playlist = os.path.join(root_directory, subdir_playlist)
    
    # Helper function to load and sample data
    def load_sample_data(path, desc):
        files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]
        sampled_files = np.random.choice(files, int(len(files) * sample_fraction), replace=False)  # Sample files withou$
        data = pd.concat([pd.read_parquet(f) for f in tqdm(sampled_files, desc=f'Reading {desc}')], ignore_index=True)
        return data
    
    # Load and sample data from each directory: fixed to 1) playlist, then track_playlist
    data_playlist = load_sample_data(path_playlist, 'playlist')
    data_track_playlist = load_sample_data(path_track_playlist, 'track_playlist')

    
    # Perform the inner join on 'playlist_id' and 'id'
    joined_data = pd.merge(data_playlist, data_track_playlist, left_on='id', right_on='playlist_id', how='inner')

    # Save the resulting dataframe
    joined_data.to_csv(output_file, index=False)
    
    return joined_data


# Usage
root_directory = '/path/to/spark-warehouse'
output_file = '/path/to/track_playlist_playlist_join.csv'
result = load_and_join_playlist(root_directory, 'track_playlist1', 'playlist', output_file)

if result is not None and not result.empty:
    print(result.head())
else:
    print("No data returned from join operations.")
