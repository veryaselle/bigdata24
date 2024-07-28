import pandas as pd

def save_unique_triples(csv_path, output_path):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Drop duplicate rows to ensure each triple is unique
    unique_df = df.drop_duplicates()

    # Save the DataFrame with unique triples to a CSV file
    unique_df.to_csv(output_path, index=False)
    print(f"Unique triples have been saved to {output_path}")

if __name__ == "__main__":
    csv_path = '/home/sc.uni-leipzig.de/ao582fpoy/bigdata24/bigdata24/triples.csv'
    output_path = '/home/sc.uni-leipzig.de/ao582fpoy/bigdata24/bigdata24/unique_triples.csv'
    save_unique_triples(csv_path, output_path)
