#
Please note:

(1) I have built this pipeline assuming a scenario where the client has requested:

	(a) Csv files to be combined and converted to parquet
	(b) Outputs to be partitioned by year
	(c) User ID to be hashed

(2) This pipeline assumes that incoming files have a definitive schema (variable names and datatypes)
    that has been confirmed with upstream clients

(3) In a real scenario, the contents in data and logs would be stored in a Cloud database.
    These can change in the configs.
    
(4) I have written this in Python code, as the task instructions specify Python only (and not PySpark etc)

(5) All data included is synthetic
