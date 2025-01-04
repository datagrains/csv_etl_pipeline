#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 08:55:09 2025

@author: DanielCheung

PROCESSING AND TRANSFORM STAGE
"""
# fmt: off
import yaml
import boto3
import numpy as np
import pandas as pd
import os

# If running locally update your working directory and run line below;
# not required in Cloudera
os.chdir('/Users/DanielCheung/Documents/csv_etl_pipeline')
from utils import utils
# fmt: on

# Retrieve config file
with open(f'pipeline/config.yaml') as f:
    config = yaml.safe_load(f)

# Retrieve salt for hashing
with open(f"{config['salt']}/salt.txt", 'r') as text:
    salt = text.read()

# Read in the csv files
for file in config['csv_files']:

    # Read in parquet from temp database
    df = pd.read_parquet(f"{config['temp']}/{file}.parquet")

    # Add year column, drop PII cols, hash required columns
    df = utils.Processing.add_year_column(df, 'Date of birth')
    df = utils.Processing.remove_pii_columns(df, config['remove_columns'])
    df = utils.Processing.hash_columns_sha256_salt(
        df, config['cols_to_hash'], salt)
    df = utils.Processing.add_sourcefile_variable(df, f"{file}")

    # Save it to the temp location
    df.to_parquet(f"{config['temp']}/{file}.parquet")
