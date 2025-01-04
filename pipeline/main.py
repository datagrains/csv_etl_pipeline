# -*- coding: utf-8 -*-

"""
Created on Fri Jan  3 09:23:38 2025

@author: DanielCheung

MAIN SCRIPT
"""

import logging
from datetime import datetime
import os
import yaml

# If running locally update your working directory and run line below;
# not required in Cloudera
os.chdir('/Users/DanielCheung/Documents/GitHub/csv_etl_pipeline')

# Retrieve config file
with open(f'pipeline/config.yaml') as f:
    config = yaml.safe_load(f)

# Get current datetime and format it as a string
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (can be DEBUG, INFO, etc.)
    # Customize log message format
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler(
            # Log to a file
            f"{config['directory']}/logs/{current_datetime}.log")
    ]
)

# Function to execute a script and handle logging


def execute_script(script_path: str, task_name: str):
    """Executes a script and logs success or failure."""
    logging.info(f'{task_name} started...')
    try:
        exec(open(script_path).read())
        logging.info(f'{task_name} completed successfully.')
    except Exception as e:
        logging.error(f'Error in {task_name}: {e}')
        raise  # Reraise the exception to stop the pipeline

# Pipeline functions


def run_pipeline():
    logging.info('Pipeline started...')

    # List of script paths and corresponding task names
    scripts = [
        ('pipeline/01_extract.py', 'Extract Data'),
        ('pipeline/02_clean.py', 'Clean Data'),
        ('pipeline/03_process.py', 'Process Data'),
        ('pipeline/04_metrics.py', 'Data Metrics'),
        ('pipeline/05_output.py', 'Output Results')
    ]

    # Execute each script
    for script_path, task_name in scripts:
        execute_script(script_path, task_name)

    logging.info('Pipeline completed.')


# Run the pipeline
if __name__ == '__main__':
    run_pipeline()
