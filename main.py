import subprocess
import os
import datetime

os.makedirs('logs', exist_ok=True)
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
log_file_path = f'logs/pipeline_{timestamp}.log'

def log(msg):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{now}] {msg}"
    print(line)
    with open(log_file_path, 'a') as f:
        f.write(line + '\n')

def run_script(script_name):
    log(f"--- Starting {script_name} ---")
    # Run script, capture output
    result = subprocess.run(['python', script_name], capture_output=True, text=True)
    # Log stdout and stderr line by line with timestamp
    for line in result.stdout.splitlines():
        log(f"{script_name} STDOUT: {line}")
    for line in result.stderr.splitlines():
        log(f"{script_name} STDERR: {line}")
    log(f"--- Finished {script_name} ---\n")

run_script('process_raw.py')
run_script('process_cleaned.py')

log("Pipeline completed.")
