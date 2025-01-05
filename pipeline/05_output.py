# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 08:59:38 2025.

@author: DanielCheung

LOAD (OUTPUT) STAGE
"""

import yaml
from utils import utils

# Retrieve config file
with open(f"config.yaml") as f:
    config = yaml.safe_load(f)

# Perform all formatting / saving methods
utils.Output.format_and_save_parquet(config)
