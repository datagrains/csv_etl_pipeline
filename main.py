# -*- coding: utf-8 -*-

"""
Created on Fri Jan  3 09:23:38 2025.

@author: DanielCheung

MAIN SCRIPT
"""
import os
import sys
import logging
import yaml
import boto3
from datetime import datetime


def set_working_directory(directory: str):
    """Set the working directory based on whether the script is running in an IDE."""
    try:
        if any(ide in sys.modules for ide in ["spyder", "IPython", "PyCharm"]):
            os.chdir(os.getcwd())
            logging.info(
                f"Working directory set to: {
                         os.getcwd()} (Running in IDE)"
            )
        else:
            logging.info("Not running in an IDE. No directory change.")
    except Exception as e:
        logging.error(f"Error occurred while setting working directory: {e}")


def load_config(config_path: str):
    """Load configuration from a YAML file."""
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
    """Set up logging configuration."""
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(
        os.getcwd(),
        "logs",
        f"{current_datetime}.log",
    )

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler(log_file),  # Log to file
        ],
    )


def execute_script(script_path: str, task_name: str):
    """Execute a script and logs its success or failure."""
    logging.info(f"{task_name} started...")
    try:
        with open(script_path) as script_file:
            exec(script_file.read())
        logging.info(f"{task_name} completed successfully.")
    except Exception as e:
        logging.error(f"Error in {task_name}: {e}")
        raise


def run_pipeline(scripts: list):
    """Run the ETL pipeline by executing each script."""
    logging.info("Pipeline started...")

    for script_path, task_name in scripts:
        execute_script(script_path, task_name)

    logging.info("Pipeline completed.")


def main():
    """Load config, set the working directory, and run the pipeline."""
    config = load_config("config.yaml")

    # Set up logging
    setup_logging(config)

    # Set the working directory if running in an IDE
    set_working_directory(os.getcwd())

    # Define the scripts and their tasks
    scripts = [
        ("pipeline/01_extract.py", "Extract Data"),
        ("pipeline/02_clean.py", "Clean Data"),
        ("pipeline/03_process.py", "Process Data"),
        ("pipeline/04_metrics.py", "Data Metrics"),
        ("pipeline/05_output.py", "Output Results"),
    ]

    # Run the pipeline
    run_pipeline(scripts)


if __name__ == "__main__":
    main()
