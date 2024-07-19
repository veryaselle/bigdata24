
import pandas as pd

def create_triples(csv_path, relationships):
    """ Generate triples from a CSV file based on specified relationships. """
    df = pd.read_csv(csv_path)
    
    # Initialize an empty list to store triples
    triples = []
    
    # Iterate over each row in the dataframe
    for _, row in df.iterrows():
        # For each relationship, create a triple if the necessary columns exist in the dataframe
        for source_col, relation, target_col in relationships:
            if source_col in df.columns and target_col in df.columns:
                source = row[source_col]
                target = row[target_col]
                if pd.notna(source) and pd.notna(target):  # Ensure non-null values
                    triples.append((source, relation, target))
    
    return triples

# Relationships definition
relationships = [
    ("album_id", "has_artist", "artist_id"),
    ("album_id", "has_track", "track_id"),
    ("album_id", "is_in", "playlist_id"),
    ("artist_id", "has_album", "album_id"),
    ("artist_id", "has_track", "track_id"),
    ("artist_id", "is_in", "playlist_id"),
    ("track_id", "comes_from", "album_id"),
    ("track_id", "comes_from", "artist_id"),
    ("track_id", "is_in", "playlist_id"),
    ("playlist_id", "has_album", "album_id"),
    ("playlist_id", "has_artist", "artist_id"),
    ("playlist_id", "has_track", "track_id")
]

# Define the path to your CSV
csv_path = '/path/to/dump.csv'

# Generate the triples
generated_triples = create_triples(csv_path, relationships)

# Optionally, convert triples to a DataFrame and save to a new CSV for inspection or further use
triples_df = pd.DataFrame(generated_triples, columns=['head', 'relation', 'tail'])
triples_df.to_csv('/path/to/triples.csv', index=False)

print("Triples have been generated and saved.")

