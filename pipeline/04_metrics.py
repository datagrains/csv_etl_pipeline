# -*- coding: utf-8 -*-
"""
Metrics and Monitoring Script for ETL Pipeline.

This script calculates and visualizes data quality metrics for both raw and processed datasets in the ETL pipeline. It reads CSV files from the source database and Parquet files from temporary storage, then computes various data quality metrics, generates visualizations (charts), and saves the results to designated output locations.

The script performs the following tasks:
- Loads the configuration from a YAML file to retrieve the list of CSV files.
- Reads raw CSV files and processed Parquet files.
- Calculates data quality metrics such as null counts, distinct values, and character lengths using the QualityMetrics utility.
- Generates visualizations (charts) for both raw and processed data quality metrics.
- Saves both the data quality metrics and visualizations to appropriate directories for future analysis.

Key functionalities:
- **Data Quality Calculation**: Computes various metrics like null percentage, distinct count, and character lengths for both raw and processed datasets.
- **Visualization**: Generates charts for raw and processed data quality metrics and saves them for reporting and analysis.
- **File Management**: Reads data from both source and temporary storage, processes it, and saves the quality metrics and visualizations to the designated output locations.

Dependencies:
- pandas
- yaml
- logging
- datetime
- utils (custom utility module)
"""

import yaml
import pandas as pd
import logging
from datetime import datetime
from utils import utils


# Retrieve config file
with open(f"config.yaml") as f:
    config = yaml.safe_load(f)

# Read in the csv files
for file in config["csv_files"]:

    # Read in csv from database
    df_source = pd.read_csv(f"{config['inputs']}/{file}.csv")
    df_processed = pd.read_parquet(f"{config['temp']}/{file}.parquet")

    # Perform all metrics methods
    quality_df_input = utils.QualityMetrics.calculate_data_quality(df_source)
    quality_df_processed = utils.QualityMetrics.calculate_data_quality(
        df_processed
    )

    # Perform all visualisation methods (e.g. charts)
    logging.info(f"Generating charts for {file}")
    utils.QualityMetrics.plot_quality_metrics(
        quality_df_input,
        save_directory=f"{config['outputs']
                          }/quality_metrics/raw/charts/{file}_raw",
    )
    utils.QualityMetrics.plot_quality_metrics(
        quality_df_processed,
        save_directory=f"{
            config['outputs']}/quality_metrics/processed/charts/{file}_processed",
    )

    # Save it to temp location
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    quality_df_input.to_parquet(
        f"{config['outputs']
           }/quality_metrics/raw/{file}_raw_{current_datetime}.parquet"
    )
    quality_df_processed.to_parquet(
        f"{config['outputs']}/quality_metrics/processed/{
            file}_processed_{current_datetime}.parquet"
    )
