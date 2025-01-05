#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 08:54:22 2025

@author: DanielCheung

EXTRACT AND PREVALIDATE STAGE
"""

# fmt: off
import yaml
import boto3
import numpy as np
import pandas as pd
import os
import logging
from utils import utils
# fmt: on

# Retrieve config file
with open(f'pipeline/config.yaml') as f:
    config = yaml.safe_load(f)

# Read in the csv files
for file in config['csv_files']:

    # Read in csv from source database
    df = pd.read_csv(f"{config['inputs']}/{file}.csv")

    # Validate functions
    logging.info(f"Validating {file}")
    utils.DataFrameValidation.variable_names(df, config)
    utils.DataFrameValidation.variable_types(df, config)
    utils.DataFrameValidation.variable_count(df, config)

    # Save it to temp location
    df.to_parquet(f"{config['temp']}/{file}.parquet")
