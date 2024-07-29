import pandas as pd
import os
from tqdm import tqdm

def load_and_join_playlist(root_directory, subdir_track_playlist, subdir_playlist, output_file, sample_fraction=0.1, chunksize=100000):
    # Define paths to the subdirectories
    path_track_playlist = os.path.join(root_directory, subdir_track_playlist)
    path_playlist = os.path.join(root_directory, subdir_playlist)
    
    # Helper function to load and sample data
    def load_sample_data(path, desc, sample_fraction):
        files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]
        if not files:
            raise FileNotFoundError(f"No parquet files found in directory: {path}")
        data = pd.concat([pd.read_parquet(f) for f in tqdm(files, desc=f'Reading {desc} ({len(files)} files)')], ignore_index=True)
        sampled_data = data.sample(frac=sample_fraction).reset_index(drop=True)  # Sample and shuffle the data
        return sampled_data

    # Load and sample data from the playlist directory
    data_playlist = load_sample_data(path_playlist, 'playlist', sample_fraction)

    # Ensure the sampled playlist dataframe is not empty
    if data_playlist.empty:
        raise ValueError("The playlist dataframe is empty after loading data.")

    # Load all track_playlist files and perform the join iteratively
    joined_track_playlist_frames = []
    track_playlist_files = [os.path.join(path_track_playlist, f) for f in os.listdir(path_track_playlist) if f.endswith('.parquet')]
    for file in tqdm(track_playlist_files, desc='Processing track_playlist files'):
        data_track_playlist = pd.read_parquet(file)
        if not data_track_playlist.empty:
            joined_data = pd.merge(data_playlist, data_track_playlist, left_on='id', right_on='playlist_id', how='inner')
            joined_track_playlist_frames.append(joined_data)

    # Concatenate all the join results for track_playlist
    if joined_track_playlist_frames:
        final_track_playlist_joined_data = pd.concat(joined_track_playlist_frames, ignore_index=True)
        # Save the resulting dataframe
        final_track_playlist_joined_data.to_csv(output_file, index=False)
        return final_track_playlist_joined_data
    else:
	    raise ValueError("No data returned from join operations with track_playlist.")

# Usage
root_directory = '/path/to/spark-warehouse'
output_file = 'track_playlist_playlist_join.csv'
try:
    result = load_and_join_playlist(root_directory, 'track_playlist1', 'playlist', output_file)
    if result is not None and not result.empty:

        print(result.head())
    else:
	    print("No data returned from join operations.")
except Exception as e:
    print(f"An error occurred: {e}")
