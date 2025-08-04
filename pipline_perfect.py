from prefect import task, flow, get_run_logger
import time
import os
import shutil
import pandas as pd

RAW_DIR = 'raw_files'
PROCESSED_DIR = os.path.join(RAW_DIR, 'processed')
CLEANED_DIR = 'cleaned_csv'
SQL_OUTPUT_DIR = 'sql_scripts'

# Ensure output directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)
os.makedirs(SQL_OUTPUT_DIR, exist_ok=True)

def is_file_locked(filepath):
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, 'a'):
            pass
        return False
    except IOError:
        return True

@task(retries=3, retry_delay_seconds=10)
def process_raw_file(filename: str):
    logger = get_run_logger()
    file_path = os.path.join(RAW_DIR, filename)

    if is_file_locked(file_path):
        logger.info(f"File {filename} is locked, retrying later...")
        raise RuntimeError("File is locked")

    logger.info(f"Processing raw file: {filename}")

    df = pd.read_csv(file_path, sep=';', skiprows=1)
    df.columns = df.columns.str.lower()

    df = df.dropna(subset=['fakt.nr'])
    df['fakt.nr'] = df['fakt.nr'].astype(int).astype(str)
    df = df[['ordernr', 'fakt.nr', 'benämning']]
    df['benämning'] = df['benämning'].str.strip()

    # Extract only the job names matching patterns
    df['benämning'] = df['benämning'].str.extract(r'^(SE-\d{2}-\d{4}|S\d{5})', expand=False)
    filtered_df = df[df['benämning'].notnull()]
    filtered_no_dupes = filtered_df.drop_duplicates(subset=['ordernr', 'benämning', 'fakt.nr'])

    cleaned_filename = f"cleaned_{filename}"
    cleaned_path = os.path.join(CLEANED_DIR, cleaned_filename)
    filtered_no_dupes.to_csv(cleaned_path, sep=';', index=False)
    logger.info(f"Cleaned file saved to {cleaned_path}")

    # Move the raw file to processed
    shutil.move(file_path, os.path.join(PROCESSED_DIR, filename))
    logger.info(f"Moved raw file {filename} to {PROCESSED_DIR}")

    return cleaned_filename

@task
def generate_sql_from_cleaned(cleaned_filename: str):
    logger = get_run_logger()
    cleaned_path = os.path.join(CLEANED_DIR, cleaned_filename)
    sql_file_path = os.path.join(SQL_OUTPUT_DIR, cleaned_filename.replace('.csv', '.sql'))

    df = pd.read_csv(cleaned_path, sep=';')

    with open(sql_file_path, 'w') as f:
        f.write('BEGIN TRANSACTION;\n\n')

        for _, row in df.iterrows():
            portal = str(row['ordernr']) if pd.notna(row['ordernr']) else ''
            invoice = str(row['fakt.nr'])
            job_name = row['benämning']

            sql = f"""
UPDATE job_header
SET 
    portal_number = CASE 
        WHEN portal_number = '' THEN '{portal}'
        WHEN CHARINDEX('{portal}', portal_number) > 0 THEN portal_number
        ELSE portal_number + ', {portal}'
    END,
    invoice_number = CASE 
        WHEN invoice_number = '' THEN '{invoice}'
        WHEN CHARINDEX('{invoice}', invoice_number) > 0 THEN invoice_number
        ELSE invoice_number + ', {invoice}'
    END
WHERE job_name = '{job_name}';
"""
            f.write(sql + '\n')

        f.write('\nCOMMIT;\n')

    logger.info(f"SQL script written to {sql_file_path}")

    # Move cleaned file to processed
    processed_cleaned_path = os.path.join(CLEANED_DIR, 'processed', cleaned_filename)
    os.makedirs(os.path.dirname(processed_cleaned_path), exist_ok=True)
    shutil.move(cleaned_path, processed_cleaned_path)
    logger.info(f"Moved cleaned file to {processed_cleaned_path}")

@flow
def invoice_pipeline():
    logger = get_run_logger()
    logger.info("Starting invoice pipeline flow")

    files = [f for f in os.listdir(RAW_DIR) if f.endswith('.csv')]
    logger.info(f"Found {len(files)} files in raw directory")

    for filename in files:
        try:
            cleaned_filename = process_raw_file.submit(filename).result()
            generate_sql_from_cleaned.submit(cleaned_filename).result()
        except Exception as e:
            logger.warning(f"Skipping file {filename} due to error: {e}")

    logger.info("Invoice pipeline flow completed")

if __name__ == "__main__":
    invoice_pipeline()
