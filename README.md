# Invoice Pipeline

This project processes raw invoice CSV files, cleans and deduplicates job data, and generates SQL scripts to update a database. It uses [Prefect](https://www.prefect.io/) for orchestration.

## Project Structure

- `pipline_perfect.py` — Main Prefect pipeline for processing and SQL generation.
- `process_raw.py` — (Legacy) Script for cleaning raw CSV files.
- `process_cleaned.py` — (Legacy) Script for generating SQL from cleaned CSVs.
- `raw_files/` — Directory for raw invoice CSV files.
  - `processed/` — Archive for processed raw files.
- `cleaned_csv/` — Directory for cleaned CSV files.
  - `processed/` — Archive for processed cleaned files.
- `sql_scripts/` — Output directory for generated SQL scripts.
- `logs/` — Log files.

## Usage

1. Place raw invoice CSV files in the `raw_files/` directory.
2. Run the pipeline:
    ```sh
    python pipline_perfect.py
    ```
3. Cleaned CSVs and SQL scripts will be generated and moved to their respective folders.

## Requirements

- Python 3.8+
- pandas
- prefect

Install dependencies:
```sh
pip install pandas prefect
```

## Notes

- The pipeline skips locked files and logs progress.
- Only job names matching `SE-XX-XXXX` or `SXXXXX` patterns are processed.
- All output and intermediate files are organized into subfolders.

## License

MIT
