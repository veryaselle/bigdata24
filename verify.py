import os
import zlib

def calculate_and_save_crc(parquet_path):
    """Calculate the CRC32 of a parquet file and save it to a corresponding .crc file."""
    try:
        crc32 = 0
        with open(parquet_path, 'rb') as f:
            while True:
                data = f.read(65536)  # Read in chunks to manage large files
                if not data:
                    break
                crc32 = zlib.crc32(data, crc32)
        crc32 &= 0xFFFFFFFF  # Ensure CRC32 is in the correct format

        crc_path = f"{parquet_path}.crc"
        with open(crc_path, 'w') as crc_file:
            crc_file.write(f"{crc32}")
        return True
    except Exception as e:
        print(f"Failed to generate CRC for {parquet_path}: {str(e)}")
        return False

def process_directory(directory):
    """Recursively process each directory for Parquet files and regenerate CRC files."""
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.parquet'):
                parquet_path = os.path.join(root, file)
                if not calculate_and_save_crc(parquet_path):
                    print(f"Error processing {parquet_path}")

if __name__ == "__main__":
    root_directory = '/path/to/spark-warehouse'
    process_directory(root_directory)
    print("CRC regeneration complete.")
