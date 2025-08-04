import pandas as pd
import os
import shutil

RAW_DIR = 'raw_files'
PROCESSED_DIR = os.path.join(RAW_DIR, 'processed')
CLEANED_DIR = 'cleaned_csv'

# Ensure output directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

# Process all files in RAW_DIR
for filename in os.listdir(RAW_DIR):
    if filename.endswith('.csv'):
        file_path = os.path.join(RAW_DIR, filename)

        # Skip if it's a file already in 'processed'
        if PROCESSED_DIR in file_path:
            continue

        print(f"Processing {filename}...")

        # Read and clean data
        df = pd.read_csv(file_path, sep=';', skiprows=1)
        df.columns = df.columns.str.lower()

        df = df.dropna(subset=['fakt.nr'])
        df['fakt.nr'] = df['fakt.nr'].astype(int).astype(str)
        df = df[['ordernr', 'fakt.nr', 'benämning']]

        # Filter SE-XX-XXXX patterns
        filtered_df = df[df['benämning'].str.contains(r'^SE-\d{2}-\d{4}$', na=False)]

        # Save cleaned output
        cleaned_filename = f"cleaned_{filename}"
        cleaned_path = os.path.join(CLEANED_DIR, cleaned_filename)
        filtered_df.to_csv(cleaned_path, sep=';', index=False)
        print(f"Cleaned file written to {cleaned_path}")

        # Move processed file to 'processed' folder
        shutil.move(file_path, os.path.join(PROCESSED_DIR, filename))
        print(f"Moved {filename} to {PROCESSED_DIR}")

print("All files processed.")
