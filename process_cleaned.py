import shutil
import os
import pandas as pd
CLEANED_DIR = 'cleaned_csv'
PROCESSED_DIR = os.path.join(CLEANED_DIR, 'processed')
SQL_OUTPUT_DIR = 'sql_scripts'

# Make sure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

for filename in os.listdir(CLEANED_DIR):
    if filename.endswith('.csv'):
        file_path = os.path.join(CLEANED_DIR, filename)
        print(f"Generating SQL for {filename}...")

        df = pd.read_csv(file_path, sep=';')
        sql_file_path = os.path.join(SQL_OUTPUT_DIR, f"{filename.replace('.csv', '')}.sql")

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

            f.write('\nCOMMIT;')

        print(f"SQL script written to {sql_file_path}")

        # Move the processed CSV file to the processed folder
        dest_path = os.path.join(PROCESSED_DIR, filename)
        shutil.move(file_path, dest_path)
        print(f"Moved {filename} to {PROCESSED_DIR}")

print("All SQL scripts generated.")
