# -*: utf-8 -*-
"""
Extract and Prevalidate Script for ETL Pipeline.

This script is responsible for extracting CSV files from the source database, performing validation checks on the DataFrame according to the configuration, and saving the validated data to a temporary location in Parquet format.

The script does the following:
- Loads the configuration from a YAML file to retrieve the list of CSV files and paths.
- Reads each CSV file from the source directory.
- Validates the extracted DataFrame based on the configuration using methods from the `utils` module.
- Saves the validated DataFrame to a temporary directory in Parquet format.

Key functionalities:
- **Validation**: Ensures the DataFrame’s column names, data types, and column count match the configuration.
- **File Management**: Reads input files, processes them, and stores them in a specified output format (Parquet).

Dependencies:
- pandas
- yaml
- logging
- utils (custom utility module)
"""

import yaml
import pandas as pd
import logging
from utils import utils

# Retrieve config file
with open(f"config.yaml") as f:
    config = yaml.safe_load(f)

# Read in the csv files
for file in config["csv_files"]:

    # Read in csv from source database
    df = pd.read_csv(f"{config['inputs']}/{file}.csv")

    # Perform all validations
    logging.info(f"Validating {file}")
    utils.DataFrameValidation.variable_names(df, config)
    utils.DataFrameValidation.variable_types(df, config)
    utils.DataFrameValidation.variable_count(df, config)

    # Save it to temp location
    df.to_parquet(f"{config['temp']}/{file}.parquet")
