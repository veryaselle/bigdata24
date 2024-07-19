import pandas as pd

# Load the CSV files
df_final = pd.read_csv('/path/to/final_join_output.csv')
df_track_playlist = pd.read_csv('/path/to/track_playlist_playlist_join.csv')

# Perform an inner join on 'track_id'
merged_df = pd.merge(df_final, df_track_playlist, on='track_id', how='inner')

# Select only the specified columns, adjusting for the renamed columns
# Change 'playlist_id', 'id', 'name' to 'playlist_id_x', 'id_x', 'name_x' or whichever is appropriate
# Confirm from the printout which columns you need; for example, using '_x' suffixes or '_y' if relevant
try:
    selected_columns = merged_df[['track_id', 'playlist_id_x', 'artist_id', 'id_x', 'name_x']]
    # Save the resulting DataFrame to a new CSV file
    selected_columns.to_csv('/path/to/dump.csv', index=False)
    print("File saved successfully.")
except KeyError as e:
    print(f"KeyError - one of the columns does not exist in the merged DataFrame: {e}")

