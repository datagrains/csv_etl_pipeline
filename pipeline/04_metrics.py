# -*- coding: utf-8 -*-

"""
Created on Fri Jan  3 08:59:38 2025

@author: DanielCheung

METRICS AND MONITORING STAGE
"""
# fmt: off
import yaml
import boto3
import numpy as np
import pandas as pd
import os
import logging


# If running locally update your working directory and run line below;
# not required in Cloudera
os.chdir('/Users/DanielCheung/Documents/GitHub/csv_etl_pipeline')
from utils import utils
# fmt: on


# Retrieve config file
with open(f'pipeline/config.yaml') as f:
    config = yaml.safe_load(f)

# Read in the csv files
for file in config['csv_files']:

    # Read in csv from database
    df_source = pd.read_csv(f"{config['inputs']}/{file}.csv")
    df_processed = pd.read_parquet(f"{config['temp']}/{file}.parquet")

    # Calculate quality metrics
    quality_df_input = utils.QualityMetrics.calculate_data_quality(df_source)
    quality_df_processed = utils.QualityMetrics.calculate_data_quality(
        df_processed)

    # Plot charts
    logging.info(f"Generating charts for {file}")
    utils.QualityMetrics.plot_quality_metrics(
        quality_df_input, save_directory=f"{config['temp']}/quality_metrics/raw/charts/{file}_raw")
    utils.QualityMetrics.plot_quality_metrics(
        quality_df_processed, save_directory=f"{config['temp']}/quality_metrics/processed/charts/{file}_processed")

    # Save it to temp location
    quality_df_input.to_parquet(
        f"{config['temp']}/quality_metrics/raw/{file}_raw.parquet")
    quality_df_processed.to_parquet(
        f"{config['temp']}/quality_metrics/processed/{file}_processed.parquet")
