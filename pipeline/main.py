# -*- coding: utf-8 -*-

"""
Created on Fri Jan  3 09:23:38 2025.

@author: DanielCheung

MAIN SCRIPT
"""

import inspect
import logging
from datetime import datetime
import os
import yaml
import sys
import boto3

# If running locally in an IDE (Spyder, Jupyter etc) you will need to update
# your working directory to /.../<filepath>/csv_etl_pipeline;
# This is not required in Cloud environments (AirFlow, CDSW, AWS Lambda etc)
directory = os.chdir('/Users/DanielCheung/Documents/GitHub/csv_etl_pipeline')


def set_working_directory(directory: str):
    """Sets the working directory based on whether the script is running in an IDE."""
    try:
        if any(ide in sys.modules for ide in ['spyder', 'IPython', 'PyCharm']):
            os.chdir(directory)
            logging.info(f"Working directory set to: {
                         os.getcwd()} (Running in IDE)")
        else:
            logging.info("Not running in an IDE. No directory change.")
    except Exception as e:
        logging.error(f"Error occurred while setting working directory: {e}")


def load_config(config_path: str):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error reading config file: {e}")
        raise


def setup_logging(config: dict):
    """Sets up logging configuration."""
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(config['directory'], 'logs', f"{
                            current_datetime}.log")

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler(log_file)  # Log to file
        ]
    )


def execute_script(script_path: str, task_name: str):
    """Executes a script and logs its success or failure."""
    logging.info(f'{task_name} started...')
    try:
        with open(script_path) as script_file:
            exec(script_file.read())
        logging.info(f'{task_name} completed successfully.')
    except Exception as e:
        logging.error(f'Error in {task_name}: {e}')
        raise


def run_pipeline(scripts: list):
    """Runs the ETL pipeline by executing each script."""
    logging.info('Pipeline started...')

    for script_path, task_name in scripts:
        execute_script(script_path, task_name)

    logging.info('Pipeline completed.')


def main():
    """Main function to load config, set the working directory, and run the pipeline."""
    config = load_config('pipeline/config.yaml')

    # Set up logging
    setup_logging(config)

    # Set the working directory if running in an IDE
    set_working_directory(config['directory'])

    # Define the scripts and their tasks
    scripts = [
        ('pipeline/01_extract.py', 'Extract Data'),
        ('pipeline/02_clean.py', 'Clean Data'),
        ('pipeline/03_process.py', 'Process Data'),
        ('pipeline/04_metrics.py', 'Data Metrics'),
        ('pipeline/05_output.py', 'Output Results')
    ]

    # Run the pipeline
    run_pipeline(scripts)


if __name__ == '__main__':
    main()
