# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 08:59:38 2025.

@author: DanielCheung

METRICS AND MONITORING STAGE
"""
import yaml
import pandas as pd
import logging
from datetime import datetime
from utils import utils


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

    # Grab current datetime
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Plot charts
    logging.info(f"Generating charts for {file}")
    utils.QualityMetrics.plot_quality_metrics(
        quality_df_input, save_directory=f"{config['outputs']}/quality_metrics/raw/charts/{file}_raw")
    utils.QualityMetrics.plot_quality_metrics(
        quality_df_processed, save_directory=f"{config['outputs']}/quality_metrics/processed/charts/{file}_processed")

    # Save it to temp location
    quality_df_input.to_parquet(
        f"{config['outputs']}/quality_metrics/raw/{file}_raw_{current_datetime}.parquet")
    quality_df_processed.to_parquet(
        f"{config['outputs']}/quality_metrics/processed/{file}_processed_{current_datetime}.parquet")
