import pandas as pd
import os
import shutil

RAW_DIR = 'raw_files'
PROCESSED_DIR = os.path.join(RAW_DIR, 'processed')
CLEANED_DIR = 'cleaned_csv'

# Ensure output directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

def is_file_locked(filepath):
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, 'a'):
            pass
        return False
    except IOError:
        return True

# Process all files in RAW_DIR
for filename in os.listdir(RAW_DIR):
    if not filename.endswith('.csv'):
        continue

    file_path = os.path.join(RAW_DIR, filename)

    # Skip files inside processed folder (in case)
    if PROCESSED_DIR in file_path:
        continue

    if is_file_locked(file_path):
        print(f"Skipping locked file {filename}")
        continue

    print(f"Processing {filename}...")

    # Read and clean data
    df = pd.read_csv(file_path, sep=';', skiprows=1)
    df.columns = df.columns.str.lower()

    df = df.dropna(subset=['fakt.nr'])
    df['fakt.nr'] = df['fakt.nr'].astype(int).astype(str)
    df = df[['ordernr', 'fakt.nr', 'benämning']]
    df['benämning'] = df['benämning'].str.strip()

    # Filter SE-XX-XXXX & SXXXXX patterns
    df['benämning'] = df['benämning'].str.extract(r'^(SE-\d{2}-\d{4}|S\d{5})', expand=False)
    filtered_df = df[df['benämning'].notnull()]


    filtered_df = filtered_df.drop_duplicates(subset=['ordernr', 'fakt.nr', 'benämning'])
    # Save cleaned output
    cleaned_filename = f"cleaned_{filename}"
    cleaned_path = os.path.join(CLEANED_DIR, cleaned_filename)
    filtered_df.to_csv(cleaned_path, sep=';', index=False)
    print(f"Cleaned file written to {cleaned_path}")

    # Move processed file to 'processed' folder
    shutil.move(file_path, os.path.join(PROCESSED_DIR, filename))
    print(f"Moved {filename} to {PROCESSED_DIR}")

print("All files processed.")
