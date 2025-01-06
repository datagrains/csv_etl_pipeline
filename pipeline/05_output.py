# -*- coding: utf-8 -*-
"""
Outputting Script for ETL Pipeline.

This script is responsible for formatting and saving the processed data into a final Parquet file after all necessary transformations have been applied in the ETL pipeline. It reads configuration details from a YAML file and then invokes the appropriate methods to handle the output process.

The script performs the following tasks:
- Loads the configuration from a YAML file to retrieve paths for CSV files, output directories, and partition columns.
- Invokes the `format_and_save_parquet` method from the `utils.Output` utility to process and save the data.

Key functionalities:
- **Format and Save Parquet**: This method processes multiple Parquet files, combines them into one, and saves the output to the final location with partitioning if specified in the configuration.

Dependencies:
- yaml
- utils (custom utility module)
"""


import yaml
from utils import utils

# Retrieve config file
with open(f"config.yaml") as f:
    config = yaml.safe_load(f)

# Perform all formatting / saving methods
utils.Output.format_and_save_parquet(config)
