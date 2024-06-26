import struct
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def read_crc_from_binary_file(file_path):
    """ Read a binary CRC file and extract the CRC value. """
    try:
        with open(file_path, 'rb') as file:
            file_content = file.read()
            if len(file_content) < 4:
                return None, f"File {file_path} too short to contain a valid CRC32 value."
            crc_value = struct.unpack('>I', file_content[-4:])[0]
            return crc_value, None
    except IOError as e:
        return None, f"Error reading CRC file {file_path}: {e}"

def process_files(file_paths):
    """ Process a list of file paths to read CRC values using multi-threading. """
    crc_values = {}
    errors = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_file = {executor.submit(read_crc_from_binary_file, path): path for path in file_paths}
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            crc_value, error = future.result()
            if error is not None:
                errors.append(error)
            else:
                crc_values[file_path] = crc_value
    return crc_values, errors

def find_crc_files(directory_path):
    """ Recursively find all .crc files in given directory and subdirectories. """
    crc_files = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.lower().endswith('.crc'):
                crc_files.append(os.path.join(root, file))
    return crc_files

# Define the root directory and subdirectories to be checked
root_directory = 'c:/Users/verya/Desktop/spark-warehouse'
subdirectories = ['artist', 'playlist', 'track', 'track_artist1', 'track_playlist1']

# Collect all CRC file paths from all specified directories
all_crc_files = []
for subdir in subdirectories:
    full_path = os.path.join(root_directory, subdir)
    all_crc_files.extend(find_crc_files(full_path))

# Report the total number of CRC files found and then process them
print(f"Total .crc files detected: {len(all_crc_files)}")
crc_values, errors = process_files(all_crc_files)
print(f"Processed {len(crc_values)} CRC files.")
if errors:
    print(f"Errors encountered: {len(errors)}")
    for error in errors:
        print(error)






    






