# CSV ETL Pipeline

## Overview
This repository contains a Python-based ETL (Extract, Transform, Load) pipeline designed to process CSV files, clean and transform data, and store it in a Parquet format. The pipeline includes functionality for data sanitization (such as the removal of Personally Identifiable Information, PII), metric generation, and visualization. The solution is built with best practices in mind, including unit tests, logging, and cloud-ready deployment.

## Features and Capabilities
- **CSV to Parquet Conversion**: Efficiently converts CSV files into the optimized Parquet file format for faster querying and reduced storage costs.
- **Data Cleaning**: Automatically handles missing values, incorrect data types, and other data quality issues.
- **Data Hashing**: Implements hashing mechanisms to anonymize sensitive information.
- **PII Removal**: Identifies and removes Personally Identifiable Information (PII) variables to ensure compliance with privacy standards.
- **Metrics and Charts**: Generates useful metrics and visualizes data in charts to facilitate analysis.
- **Unit Testing**: Includes comprehensive unit tests to ensure pipeline integrity and reliability.
- **Logging**: Implements robust logging for troubleshooting, auditing, and tracking pipeline progress.
- **CI/CD Cloud-Ready**: Configured for deployment in cloud environments, with integration potential for services like AWS, Azure, and others.

## Assumptions
- The pipeline assumes that incoming CSV files conform to a definitive schema, which includes known variable names and data types. This schema should be confirmed with upstream clients before pipeline execution.
- All data used in this pipeline is **synthetic** and provided for demonstration purposes.
- Example data and log files are included to demonstrate how the pipeline operates with actual inputs.

## Technology Stack
- **Python**: The pipeline is written in Python, in accordance with specified requirements to avoid the use of PySpark or other external frameworks.
- **Docker**: A `Dockerfile` is included to containerize the pipeline, enabling seamless deployment and testing.
- **Parquet**: The pipeline outputs data in the Parquet format, ensuring efficient storage and faster read times for big data analytics.

## Usage

### Running Locally
1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/csv-etl-pipeline.git
   cd csv-etl-pipeline
   ```

2. Build the Docker image:
   ```bash
   docker build -t csv-etl-pipeline .
   ```

3. Run the pipeline within Docker:
   ```bash
   docker run -v /path/to/csv/files:/data csv-etl-pipeline
   ```

## Running in the Cloud
The pipeline is designed to be deployable in cloud environments like AWS and Azure. It can be integrated into CI/CD workflows to automate data processing tasks in cloud-based environments.

## Assumptions

The pipeline assumes that incoming CSV files conform to a definitive schema, which includes known variable names and data types. This schema should be confirmed with upstream clients before pipeline execution.

All data used in this pipeline is synthetic and provided for demonstration purposes.

Example data and log files are included to demonstrate how the pipeline operates with actual inputs.

## Technology Stack

- **Python**: The pipeline is written in Python, in accordance with specified requirements to avoid the use of PySpark or other external frameworks.

- **Docker**: A Dockerfile is included to containerize the pipeline, enabling seamless deployment and testing.

- **Parquet**: The pipeline outputs data in the Parquet format, ensuring efficient storage and faster read times for big data analytics.
Usage


## Testing

Unit tests are included in the tests/ directory. To run the tests locally:

```python
python -m unittest discover tests/
```

## Logging

The pipeline includes a logging mechanism that captures critical information during execution. Logs are stored locally and can be configured to be sent to cloud storage or a centralized logging service.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

