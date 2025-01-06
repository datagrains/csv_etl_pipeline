# -*- coding: utf-8 -*-
"""
Processing and Transformation Stage Script for ETL Pipeline.

This script handles the processing and transformation tasks in the ETL pipeline. It reads Parquet files from the temporary storage, applies transformations such as adding columns, removing sensitive information, and hashing specific columns, and saves the transformed data back to the same location in Parquet format.

The script performs the following tasks:
- Loads the configuration from a YAML file to retrieve the list of CSV files, columns to hash, and columns to remove.
- Reads Parquet files from the temporary storage.
- Applies transformations such as adding a 'year' column, removing PII (Personally Identifiable Information) columns, hashing specified columns using a salt, and adding a source file variable.
- Saves the transformed DataFrame to the same temporary location.

Key functionalities:
- **Data Transformation**: Adds new columns, removes sensitive data, hashes specified columns, and tags the data with the source file name.
- **File Management**: Reads data from temporary storage, processes it, and saves the transformed data back.

Dependencies:
- pandas
- yaml
- utils (custom utility module)
"""

import yaml
import pandas as pd
from utils import utils

# Retrieve config file
with open(f"config.yaml") as f:
    config = yaml.safe_load(f)

# Retrieve salt for hashing
with open(f"{config['salt']}/salt.txt", "r") as text:
    salt = text.read()

# Read in the csv files
for file in config["csv_files"]:

    # Read in parquet from temp database
    df = pd.read_parquet(f"{config['temp']}/{file}.parquet")

    # Perform all processing methods
    df = utils.Processing.add_year_column(df, "Date of birth")
    df = utils.Processing.remove_pii_columns(df, config["remove_columns"])
    df = utils.Processing.hash_columns_sha256_salt(
        df, config["cols_to_hash"], salt)
    df = utils.Processing.add_sourcefile_variable(df, f"{file}")

    # Save it to the temp location
    df.to_parquet(f"{config['temp']}/{file}.parquet")
