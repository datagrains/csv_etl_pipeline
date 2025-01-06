# -*- coding: utf-8 -*-"""This module provides a set of classes and methods for data processing, validation, cleaning, and quality metrics generation for DataFrame operations.Key functionality Classes include:1. **DataFrame Validation**:   - Validate the structure of DataFrames against configuration dictionaries, checking for matching variable names, types, and counts.2. **Data Cleaning**:   - Methods to clean DataFrames by removing special characters, whitespace, and converting column values to uppercase.3. **Data Processing**:   - Includes functionality for adding new columns (e.g., year from a date column), removing PII (Personally Identifiable Information) columns, and hashing specified columns with SHA-256.4. **Quality Metrics**:   - Calculates various data quality metrics including row counts, null percentages, distinct values, maximum and minimum column lengths, and statistical summaries for numeric columns.   - Generates visual plots for these quality metrics.5. **Output Handling**:   - Handles the processing of multiple Parquet files, combining them, and saving the result to a specified output location.Created on: Fri Jan 3 09:23:38 2025@author: DanielCheung"""import osimport sysimport pandas as pdimport reimport hashlibimport loggingimport matplotlib.pyplot as pltimport seaborn as snsimport warningsimport boto3from datetime import datetimeclass DataFrameValidation:    """    A class for validating DataFrame structures against configuration dictionaries.    """    @staticmethod    def variable_names(df, config) -> bool:        """        Validates whether the column names of a DataFrame align with the keys in a configuration dictionary.        Parameters:            df (pd.DataFrame): The DataFrame whose variable names are being validated.            config (dict):  The configuration dictionary containing expected variable keys.        Returns:            bool: True if columns align, False otherwise.        """        if list(df.columns) == list(config['variables'].keys()):            logging.info(                f"SUCCESS: Variable names align between config and dataframe.")            return True        else:            logging.info(                f"Please check that the correct variables are included in both the table and the config.")            return False    @staticmethod    def variable_types(df, config) -> bool:        """        Validates whether the data types of the columns in a DataFrame align with the types specified in the configuration dictionary.        Parameters:            df (pd.DataFrame): The DataFrame whose column types are being validated.            config (dict): A dictionary containing the expected variable types. The values of the 'variables' key in the dictionary should represent the expected data types for each variable.        Returns:            bool: True if the column types in the DataFrame align with the expected types in the config, False otherwise.        Logs a success message if the types match, or a warning if there is a mismatch.        """        if df.dtypes.tolist() == list(config['variables'].values()):            logging.info(                "SUCCESS: Variable types align between config and dataframe.")            return True        else:            logging.warning(                "Please check that the correct types are consistent in both the table and the config.")            return False    @staticmethod    def variable_count(df, config) -> bool:        """        Validates whether the number of columns in a DataFrame matches the number of expected variables in a configuration dictionary.        Parameters:            df (pd.DataFrame): The DataFrame to validate.            config (dict): The configuration dictionary containing expected variable keys.        Returns:            bool: True if the number of columns matches the number of expected variables, False otherwise.        """        expected_variable_count = len(config['variables'])        actual_variable_count = len(df.columns)        if actual_variable_count == expected_variable_count:            logging.info(f"SUCCESS: Number of variables matches:{actual_variable_count}.")            return True        else:            error_message = (f"ERROR: Mismatch in variable count. "                             f"Expected: {expected_variable_count}, Found: {actual_variable_count}.")            logging.error(error_message)            raise ValueError(error_message)class Cleaning:    @staticmethod    def remove_special_characters(df: pd.DataFrame, column_name: str) -> pd.DataFrame:        """        Removes special characters from a specific column in the DataFrame.        Parameters:            df (pd.DataFrame): The DataFrame containing the column to clean.            column_name (str): The name of the column from which special characters will be removed.        Returns:            pd.DataFrame: A DataFrame with special characters removed from the specified column.        """        # Use regex to remove all non-alphanumeric characters (except spaces)        df[column_name] = df[column_name].apply(            lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x)))        return df    @staticmethod    def remove_whitespaces(df: pd.DataFrame) -> pd.DataFrame:        """        Removes whitespaces from all columns in the DataFrame.        Parameters:            df (pd.DataFrame): The DataFrame to clean.        Returns:            pd.DataFrame: The DataFrame with whitespaces removed from all columns.        """        # Apply whitespace removal to all columns        df = df.applymap(lambda x: ''.join(str(x).split()))        return df    @staticmethod    def convert_columns_uppercase(df: pd.DataFrame, columns: list) -> pd.DataFrame:        """        Converts all values in specified columns to uppercase.        Parameters:            df (pd.DataFrame): The DataFrame containing the columns to convert.            columns (list): A list of column names to convert to uppercase.        Returns:            pd.DataFrame: A DataFrame with the specified columns' values in uppercase.        """        for column in columns:            df[column] = df[column].apply(lambda x: str(x).upper())        return dfclass Processing:    @staticmethod    def add_year_column(df: pd.DataFrame, date_column: str) -> pd.DataFrame:        """        Adds a new 'year' column to the DataFrame extracted from the provided date column.        Parameters:            df (pd.DataFrame): The DataFrame containing the date column.            date_column (str): The name of the date column in 'YYYY-MM-DD' format.        Returns:            pd.DataFrame: A DataFrame with the new 'year' column.        """        # Ensure the date column is in datetime format        df[date_column] = pd.to_datetime(df[date_column])        # Create a new 'year' column by extracting the year from the date column        df['Year of birth'] = df[date_column].dt.year        return df    @staticmethod    def remove_pii_columns(df: pd.DataFrame, pii_columns: list) -> pd.DataFrame:        """        Removes columns from the DataFrame that are considered PII (Personally Identifiable Information).        Parameters:            df (pd.DataFrame): The DataFrame from which PII columns will be removed.            pii_columns (list): A list of column names to be removed from the DataFrame.        Returns:            pd.DataFrame: A DataFrame with the specified PII columns removed.        """        # Remove the PII columns if they exist in the DataFrame        df = df.drop(columns=[col for col in pii_columns if col in df.columns])        return df    @staticmethod    def hash_columns_sha256_salt(df: pd.DataFrame, columns: list, salt: str) -> pd.DataFrame:        """        Hashes columns in the DataFrame using SHA-256 with a salt.        Parameters:            df (pd.DataFrame): The DataFrame containing the columns to hash.            columns (list): A list of column names to hash.            salt (str): The salt value.        Returns:            pd.DataFrame: The DataFrame with new columns containing the hashed values.        """        for column in columns:            df[f'{column}_hashed'] = df[column].apply(                lambda x: hashlib.sha256(                    f'{x}{salt}'.encode('utf-8')).hexdigest()            )            df.drop(columns=[f"{column}"], inplace=True)        return df    @staticmethod    def add_sourcefile_variable(df, default_value=None) -> pd.DataFrame:        """        Adds a new column to the DataFrame with a default value.        Parameters:        df (pd.DataFrame): The DataFrame to which the column will be added.        column_name (str): The name of the new column.        default_value: The value to initialize the new column with. Defaults to None.        Returns:        pd.DataFrame: The updated DataFrame with the new column added.        """        df["source_file"] = default_value        return dfclass QualityMetrics:    @staticmethod    def calculate_data_quality(df: pd.DataFrame) -> pd.DataFrame:        """Calculates various data quality metrics for a DataFrame."""        # (1) Total row counts        total_rows = len(df)        # (2) Null counts and percentage        null_counts = df.isnull().sum()        null_percentage = (null_counts / total_rows) * 100        # (3) Distinct counts and percentage        distinct_counts = df.nunique()        distinct_percentage = (distinct_counts / total_rows) * 100        # (4) Maximum character length per column        max_length = df.apply(lambda x: x.astype(str).str.len().max())        # (5) Minimum character length per column        min_length = df.apply(lambda x: x.astype(str).str.len().min())        # (6) For numeric columns: max, min, mean, and std        numeric_metrics = df.select_dtypes(            include=['number']).agg(['max', 'min', 'mean', 'std'])        # Prepare a DataFrame to consolidate the results        summary = pd.DataFrame({            'Total Count': total_rows,            'Null Count': null_counts,            'Null Percentage (%)': null_percentage,            'Distinct Count': distinct_counts,            'Distinct Percentage (%)': distinct_percentage,            'Max Length': max_length,            'Min Length': min_length,        }).T        # Add numeric-specific statistics to summary        summary = pd.concat([summary, numeric_metrics.T], axis=0)        return summary    @staticmethod    def suppress_warnings():        """Suppresses warnings and console messages."""        warnings.filterwarnings("ignore")        sns.set(rc={"figure.max_open_warning": 0})  # Suppress Seaborn warnings    @staticmethod    def get_plot_customizations():        """Returns a dictionary of global customization options."""        return {            "title_fontsize": 16,            "label_fontsize": 12,            "tick_fontsize": 10,            "palette": sns.color_palette("Spectral", as_cmap=False),            "figsize": (12, 18),            "style": "whitegrid"        }    @staticmethod    def plot_quality_metrics(df: pd.DataFrame, save_directory: str = './charts/') -> None:        """Generates and saves a single chart with subplots for quality metrics."""        # Suppress warnings and messages        QualityMetrics.suppress_warnings()        # Ensure the save directory exists        os.makedirs(save_directory, exist_ok=True)        # Drop unnecessary columns        quality_metrics = df.drop(columns=['max', 'min', 'mean', 'std'])        # Customizations        customizations = QualityMetrics.get_plot_customizations()        sns.set_theme(style=customizations["style"])        fig, axes = plt.subplots(3, 1, figsize=customizations["figsize"])        # Metrics and their titles        metrics = [            ('Null Percentage (%)', 'Null Percentage by Column'),            ('Distinct Percentage (%)', 'Distinct Percentage by Column'),            ('Max Length', 'Max Length by Column')        ]        # Loop through metrics to create subplots        for ax, (metric, title) in zip(axes, metrics):            sns.barplot(                x=quality_metrics.columns,                y=quality_metrics.loc[metric],                palette=customizations["palette"],                ax=ax            )            ax.set_title(                title, fontsize=customizations["title_fontsize"], fontweight='bold')            ax.set_ylabel(metric, fontsize=customizations["label_fontsize"])            ax.set_xticklabels(quality_metrics.columns, rotation=45,                               fontsize=customizations["tick_fontsize"])        # Get current datetime and format it as a string        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")        plt.tight_layout()        plt.savefig(os.path.join(save_directory,                    f'combined_quality_metrics_{current_datetime}.png'))        plt.close()class Output:    @staticmethod    def format_and_save_parquet(config):        """        Processes multiple parquet files, combines them, and saves the result to a final location.        Parameters:            config (dict): Configuration dictionary containing:                - 'csv_files': List of base file names (without extension).                - 'temp': Directory containing the parquet files.                - 'outputs': Directory to save the final parquet file.                - 'output_asset_name': Base name for the output file.                - 'partition_columns': List of columns to use for partitioning.        Returns:            None        """        # Get list of parquet files        parquet_files = [f"{x}.parquet" for x in config['csv_files']]        df_list = []  # List to hold individual DataFrames        for file in parquet_files:            # Read each parquet file            file_path = f"{config['temp']}/{file}"            df = pd.read_parquet(file_path)            df_list.append(df)        # Combine all DataFrames        df_combined = pd.concat(df_list, ignore_index=True)        # Get current datetime and format it as a string        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")        # Save it to final location        output_path = f"{config['outputs']}/{config['output_asset_name']}_{current_datetime}.parquet"        df_combined.to_parquet(output_path,                               partition_cols=config['partition_columns'],                               engine='pyarrow')        print(f"SUCCESS: Combined parquet file saved at {output_path}")