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
    def load_sample_data(path, desc, sample_fraction):
        files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]
        if not files:
            raise FileNotFoundError(f"No parquet files found in directory: {path}")
        sample_size = max(1, int(len(files) * sample_fraction))  # Ensure at least one file is sampled
        sampled_files = np.random.choice(files, sample_size, replace=False)  # Sample files without replacement
        data = pd.concat([pd.read_parquet(f) for f in tqdm(sampled_files, desc=f'Reading {desc} ({len(sampled_files)}/{len(files)} files)')], ignore_index=True)
        return data

    # Load and sample data from the track directory
    data_track = load_sample_data(path_track, 'track', sample_fraction)

    # Ensure the sampled track dataframe is not empty
    if data_track.empty:
        raise ValueError("The track dataframe is empty after loading data.")

    # Load all track_playlist files and perform the join iteratively
    joined_track_playlist_frames = []
    track_playlist_files = [os.path.join(path_track_playlist, f) for f in os.listdir(path_track_playlist) if f.endswith('.parquet')]
    for file in tqdm(track_playlist_files, desc='Processing track_playlist files'):
        data_track_playlist = pd.read_parquet(file)
        if not data_track_playlist.empty:
            joined_data = pd.merge(data_track, data_track_playlist, left_on='id', right_on='track_id', how='inner')
            joined_track_playlist_frames.append(joined_data)

    # Concatenate all the join results for track_playlist
    if joined_track_playlist_frames:
        final_track_playlist_joined_data = pd.concat(joined_track_playlist_frames, ignore_index=True)
    else:
	    raise ValueError("No data returned from join operations with track_playlist.")

    # Load all track_artist files and perform the join iteratively
    joined_track_artist_frames = []
    track_artist_files = [os.path.join(path_track_artist, f) for f in os.listdir(path_track_artist) if f.endswith('.parquet')]
    for file in tqdm(track_artist_files, desc='Processing track_artist files'):
        data_track_artist = pd.read_parquet(file)
        if not data_track_artist.empty:
            joined_data = pd.merge(final_track_playlist_joined_data, data_track_artist, on='track_id', how='inner')
            joined_track_artist_frames.append(joined_data)

    # Concatenate all the join results for track_artist
    if joined_track_artist_frames:
        final_joined_data = pd.concat(joined_track_artist_frames, ignore_index=True)
        # Save the resulting dataframe
        final_joined_data.to_csv(output_file, index=False)
        return final_joined_data
    else:
	    raise ValueError("No data returned from join operations with track_artist.")

# Usage
root_directory = '/path/to/spark-warehouse'
output_file = 'path/to/final_join_output.csv'
try:
    result = load_and_join(root_directory, 'track_playlist1', 'track_artist1', 'track', output_file)
    if result is not None and not result.empty:
        print(result.head())
    else:
	    print("No data returned from join operations.")
except Exception as e:
    print(f"An error occurred: {e}")
