# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 08:55:09 2025.

@author: DanielCheung

PROCESSING AND TRANSFORM STAGE
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
