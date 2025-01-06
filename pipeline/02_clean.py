# -*- coding: utf-8 -*-
"""
Data Cleaning Script for ETL Pipeline.

This script is responsible for performing data cleaning tasks on the extracted and prevalidated data. It reads Parquet files from the temporary storage, applies various cleaning methods, and then saves the cleaned data back to the same location in Parquet format.

The script performs the following tasks:
- Loads the configuration from a YAML file to retrieve the list of CSV files and paths.
- Reads Parquet files from the temporary storage.
- Applies cleaning operations such as converting specified columns to uppercase and removing whitespaces from all columns.
- Saves the cleaned DataFrame to the same temporary location.

Key functionalities:
- **Data Cleaning**: Converts specified columns to uppercase and removes whitespaces from all columns.
- **File Management**: Reads data from temporary storage, processes it, and saves the cleaned data.

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

# Read in the csv files
for file in config["csv_files"]:

    # Read in csv from temp database
    df = pd.read_parquet(f"{config['temp']}/{file}.parquet")

    # Perform all cleaning methods
    df = utils.Cleaning.convert_columns_uppercase(df, config["uppercase"])
    df = utils.Cleaning.remove_whitespaces(df)

    # Save it to the temp location
    df.to_parquet(f"{config['temp']}/{file}.parquet")
