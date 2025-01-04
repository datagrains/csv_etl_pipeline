# -*- coding: utf-8 -*-

"""
Created on Fri Jan  3 08:59:38 2025

@author: DanielCheung

LOAD (OUTPUT) STAGE
"""

# fmt: off
import yaml
import boto3
import numpy as np
import pandas as pd
import os
from datetime import datetime
# If running locally update your working directory and run line below;
# not required in Cloudera
os.chdir('/Users/DanielCheung/Documents/GitHub/csv_etl_pipeline')
from utils import utils
# fmt: on


# Retrieve config file
with open(f'pipeline/config.yaml') as f:
    config = yaml.safe_load(f)

# Perform formatting and save
utils.Output.format_and_save_parquet(config)
