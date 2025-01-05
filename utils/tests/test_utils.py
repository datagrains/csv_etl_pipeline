# -*- coding: utf-8 -*-
# fmt: off
import os
# If running locally update your working directory and run line below;
# not required in Cloudera
os.chdir('/Users/DanielCheung/Documents/csv_etl_pipeline/')
from utils import utils

from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
import re
import hashlib
from datetime import datetime
import os
# fmt: on


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "date_of_birth": ["1997-01-01", "1992-01-01", "1987-01-01"]
    })


@pytest.fixture
def sample_config():
    return {
        "variables": {
            "name": "object",
            "age": "int64",
            "date_of_birth": "object"
        }
    }

# Tests for utils.DataFrameValidation


def test_variable_names(sample_dataframe, sample_config):
    assert utils.DataFrameValidation.variable_names(
        sample_dataframe, sample_config) is True

    invalid_config = {
        "variables": {
            "full_name": "object",
            "age": "int64",
            "dob": "object"
        }
    }
    assert utils.DataFrameValidation.variable_names(
        sample_dataframe, invalid_config) is False


def test_variable_types(sample_dataframe, sample_config):
    assert utils.DataFrameValidation.variable_types(
        sample_dataframe, sample_config) is True

    sample_dataframe["age"] = sample_dataframe["age"].astype("float64")

    assert utils.DataFrameValidation.variable_types(
        sample_dataframe, sample_config) is False


def test_variable_count(sample_dataframe, sample_config):
    assert utils.DataFrameValidation.variable_count(
        sample_dataframe, sample_config) is True

    invalid_dataframe = sample_dataframe.drop(columns=["date_of_birth"])
    with pytest.raises(ValueError):
        utils.DataFrameValidation.variable_count(
            invalid_dataframe, sample_config)

# Tests for utils.Cleaning


def test_remove_special_characters(sample_dataframe):
    sample_dataframe["name"] = ["A!lice", "Bo@b", "Charl#ie"]
    cleaned_df = utils.Cleaning.remove_special_characters(
        sample_dataframe, "name")
    assert cleaned_df["name"].tolist() == ["Alice", "Bob", "Charlie"]


def test_remove_whitespaces():
    df = pd.DataFrame({"col1": [" A l i c e  "], "col2": [" B o b "]})
    cleaned_df = utils.Cleaning.remove_whitespaces(df)
    assert cleaned_df.iloc[0].tolist() == ["Alice", "Bob"]


def test_convert_columns_uppercase(sample_dataframe):
    sample_dataframe["name"] = ["alice", "bob", "charlie"]
    updated_df = utils.Cleaning.convert_columns_uppercase(
        sample_dataframe, ["name"])
    assert updated_df["name"].tolist() == ["ALICE", "BOB", "CHARLIE"]

# Tests for utils.Processing


def test_add_year_column(sample_dataframe):
    updated_df = utils.Processing.add_year_column(
        sample_dataframe, "date_of_birth")
    assert "Year of birth" in updated_df.columns
    assert updated_df["Year of birth"].tolist() == [1997, 1992, 1987]


def test_remove_pii_columns(sample_dataframe):
    pii_columns = ["name"]
    updated_df = utils.Processing.remove_pii_columns(
        sample_dataframe, pii_columns)
    assert "name" not in updated_df.columns


def test_hash_columns_sha256_salt(sample_dataframe):
    salt = "12345"
    updated_df = utils.Processing.hash_columns_sha256_salt(
        sample_dataframe, ["name"], salt)
    assert "name_hashed" in updated_df.columns
    assert "name" not in updated_df.columns


def test_add_sourcefile_variable(sample_dataframe):
    updated_df = utils.Processing.add_sourcefile_variable(
        sample_dataframe, default_value="file1.csv")
    assert "source_file" in updated_df.columns
    assert updated_df["source_file"].tolist(
    ) == ["file1.csv", "file1.csv", "file1.csv"]

# Tests for utils.QualityMetrics


def test_calculate_data_quality(sample_dataframe):
    quality_summary = utils.QualityMetrics.calculate_data_quality(
        sample_dataframe)
    assert "Total Count" in quality_summary.index
    assert quality_summary.loc["Total Count"].iloc[0] == len(sample_dataframe)
