#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 08:54:52 2025

@author: DanielCheung

CLEANING STAGE
"""
# fmt: off
import yaml
import boto3
import numpy as np
import pandas as pd
import os

from utils import utils
# fmt: on

# Retrieve config file
with open(f'pipeline/config.yaml') as f:
    config = yaml.safe_load(f)

# Read in the csv files
for file in config['csv_files']:

    # Read in csv from temp database
    df = pd.read_parquet(f"{config['temp']}/{file}.parquet")

    # Remove PIIs, fix whitespaces, special characters
    df = utils.Cleaning.convert_columns_uppercase(df, config['uppercase'])
    df = utils.Cleaning.remove_whitespaces(df)

    # Save it to the temp location
    df.to_parquet(f"{config['temp']}/{file}.parquet")
