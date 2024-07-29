import pandas as pd
from tqdm import tqdm

def extract_sampled_shuffled_and_join(input_file_final, input_file_track_playlist, output_file, initial_sample_size=200000, final_sample_size=80000, chunksize=100000):
    def sample_chunks(file_path, sample_size, chunksize):
        total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract 1 for the header row
        sampled_data = []

        with tqdm(total=total_rows, desc=f'Processing {file_path}') as pbar:
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                sampled_data.append(chunk)
                pbar.update(len(chunk))

        combined_data = pd.concat(sampled_data).sample(frac=1).reset_index(drop=True)
        return combined_data.sample(n=min(sample_size, len(combined_data)))

    # Sample and shuffle the data from both files
    sampled_df_final = sample_chunks(input_file_final, initial_sample_size, chunksize)
    sampled_df_track_playlist = sample_chunks(input_file_track_playlist, initial_sample_size, chunksize)

    # Perform the join
    merged_df = pd.merge(sampled_df_final, sampled_df_track_playlist, on='track_id', how='inner').drop_duplicates().reset_index(drop=True)

    # If the merged data is larger than the final_sample_size, sample down to final_sample_size
    if len(merged_df) > final_sample_size:
        merged_df = merged_df.sample(n=final_sample_size).reset_index(drop=True)

    # Select only the specified columns
    try:
        selected_columns = merged_df[['track_id', 'playlist_id', 'artist_id', 'id_x', 'name_x']]
        # Save the resulting DataFrame to a new CSV file
        selected_columns.to_csv(output_file, index=False)
        print("File saved successfully.")
    except KeyError as e:
        print(f"KeyError - one of the columns does not exist in the merged DataFrame: {e}")

# Usage
input_file_final = '/path/to/final_join_output.csv'
input_file_track_playlist = '/path/to/track_playlist_playlist_join.csv'
output_file = '/path/to/dump.csv'

extract_sampled_shuffled_and_join(input_file_final, input_file_track_playlist, output_file)

