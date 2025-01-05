## Pipeline converting csv files to parquet files:
(1) I have built this pipeline assuming a scenario where according to the following rules:

	(a) Csv files to be combined and converted to parquet
	(b) Outputs to be partitioned by year
	(c) User ID to be hashed
 	(d) PII variables to be dropped
  	(e) Metrics with charts on data quality generated 
 
(2) This pipeline assumes that incoming files have a definitive schema (variable names and datatypes)
    that has been confirmed with upstream clients.
(3) Contents in data and logs are worked examples. 
(4) This pipeline is written in pure Python, as per the instructions which specify Python only (and not PySpark etc)
(5) All data included is synthetic
(6) A Dockerfile has been included and succesfully tested in Docker; pipeline is deployable to Cloud environments like AWS and Azure

## How to use
(1) Run the main.py script which executes the pipeline from start to finish
(2) Users of the pipeline simply need to change the config.yaml parameters. No other code changes are required.
(2) New features can be easily added in for each stage
