# -*- coding: utf-8 -*-

"""
Main script for running the ETL (Extract, Transform, Load) pipeline.

This script orchestrates the execution of a sequence of tasks (scripts) defined in a configuration file. It sets the working directory, loads the configuration, configures logging, and runs the ETL pipeline scripts in order. 

Functions included in the module:
- **load_config(config_path: str)**: Loads the configuration from a YAML file to retrieve necessary settings for the pipeline.
- **setup_logging(config: dict)**: Sets up the logging configuration, including logging to both the console and a log file.
- **execute_script(script_path: str, task_name: str)**: Executes a given script and logs its success or failure.
- **run_pipeline(scripts: list)**: Executes the entire ETL pipeline by running each task/script in the specified order.
- **main()**: The main function that loads the configuration, sets the working directory, and runs the pipeline.

Created on: Fri Jan 3 09:23:38 2025
@author: DanielCheung
"""
import os
import logging
import yaml
from datetime import datetime

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
