# CSV ETL Pipeline

## Overview
This repository contains a Python-based ETL (Extract, Transform, Load) pipeline designed to process CSV files, clean and transform data, and store it in a Parquet format. 

The pipeline includes functionality for data cleaning (such as the removal of Personally Identifiable Information, PII), metric generation, and visualisation. The solution is built with best practices in mind, including unit tests, logging, and cloud-ready deployment.

## Features and Capabilities
- **CSV to Parquet Conversion**: Efficiently converts CSV files into the optimised Parquet file format for faster querying and reduced storage costs.
- **Data Cleaning**: Automatically handles missing values, incorrect data types, and other data quality issues.
- **Data Hashing**: Implements hashing mechanisms to anonymise sensitive information.
- **PII Removal**: Identifies and removes Personally Identifiable Information (PII) variables to ensure compliance with privacy standards.
- **Metrics and Charts**: Generates useful metrics and visualises data in charts to facilitate analysis.
- **Unit Testing**: Includes comprehensive unit tests to ensure pipeline integrity and reliability.
- **Logging**: Implements robust logging for troubleshooting, auditing, and tracking pipeline progress.
- **CI/CD Cloud-Ready**: Configured for deployment in cloud environments, with integration potential for services like AWS, Azure, and others.

## Assumptions
- The pipeline assumes that incoming CSV files conform to a definitive schema, which includes known variable names and data types. This schema should be confirmed with upstream clients before pipeline execution.
- All data used in this pipeline is **synthetic** and provided for demonstration purposes.
- Example data and log files are included to demonstrate how the pipeline operates with actual inputs.
- Example data and log files are included to demonstrate how the pipeline operates with actual inputs.

## Technology Stack
- **Python**: The pipeline is written in Python, in accordance with specified requirements to avoid the use of PySpark or other external frameworks.
- **Docker**: A `Dockerfile` is included to containerise the pipeline, enabling seamless deployment and testing.
- **Parquet**: The pipeline outputs data in the Parquet format, ensuring efficient storage and faster read times for big data analytics.

## How to run

### Running locally

1. **Clone repo**: First, clone the repository to your local machine and set the current directory to the repo path:

   ```python
   git clone https://github.com/datagrains/csv_etl_pipeline.git
   cd csv_etl_pipeline
   ```

3. **Install Dependencies**: Make sure you have the required dependencies installed. You can install the dependencies using pip: 
   ```python
   pip install -r requirements.txt
   ```

4. **Configure the config.yaml**: A worked example is already included for testing. You can configure the config.yaml file according to your needs; open the `config.yaml` file in a text editor and adjust the settings as needed. 

6. **Run the Program** : Once your `config.yaml` is configured, you can run the program by executing the `main.py` file:

   ```bash
   python main.py
   ```
   This will start the program and it will use the settings you configured in `config.yaml`.

   (Note that if running on Mac OS with an IDE like Spyder or Jupyter, you may need to install Xcode Command Line Tools to interact with Git.
   Ensure your working directory is set to ../csv_etl_pipeline in your IDE, and prefix all terminal commands with `!`).

### Running via Docker
A `Dockerfile` is included to easily containerise the pipeline and run it in any environment with Docker support (e.g. AWS, Azure, Google Cloud).

1. Clone this repository:
   ```bash
   git clone https://github.com/datagrains/csv_etl_pipeline.git
   cd csv-etl-pipeline
   ```

2. Build the Docker image:
   ```bash
   docker build -t csv-etl-pipeline .
   ```

3. Run the pipeline within Docker:
   ```bash
   docker run -v csv-etl-pipeline
   ```

## CI/CD with GitHub Actions

GitHub Actions for Continuous Integration (CI) has been used to automatically run tests on every push and pull request. This ensures that the code is always tested before being merged into the main branch.

### Workflow Details
The GitHub Actions workflow files are configured in the .github/workflows/ folder.

### Steps in the Workflow:

- **Code checkout**: The workflow starts by checking out the latest code from the repository.

- **Set up Python**: It then sets up the Python environment with the specified Python version.

- **Install dependencies**: The required dependencies, including pytest, are installed using pip install -r requirements.txt.

- **Run tests**: The pytest and pylint tests are executed. For pytest, any unit test that fails will cause the workflow to fail. For pylint, a threshold of 7.0 has been set and a linting score below that will cause workflow failure. 

- **Report results**: Results are reported directly on the GitHub Actions page.




## Technology Stack

- **Python**: The pipeline is written in Python, in accordance with specified requirements to avoid the use of PySpark or other external frameworks.

- **Docker**: A Dockerfile is included to containerise the pipeline, enabling seamless deployment and testing.

- **Parquet**: The pipeline outputs data in the Parquet format, ensuring efficient storage and faster read times for big data analytics.
Usage


## Testing

Unit tests are included in the tests/ directory. To run the tests locally:

```python
pip install pytest
pytest
```

## Logging

The pipeline includes a logging mechanism that captures critical information during execution. Logs are stored locally and can be configured to be sent to cloud storage or a centralised logging service.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

