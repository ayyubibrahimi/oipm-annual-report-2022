import re
import os

import pandas as pd


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Removes unnamed columns and convert column names to snake case

    Args:
        df (pd.DataFrame):
            the frame to process

    Returns:
        the updated frame
    """
    df = df[[col for col in df.columns if not col.startswith("Unnamed:")]]
    df.columns = [
        re.sub(r"[\s\W]+", "_", col.strip()).lower().strip("_") for col in df.columns
    ]
    return df

def set_values(df: pd.DataFrame, value_dict: dict) -> pd.DataFrame:
    """Set entire column to a value.

    Multiple columns can be specified each as a single key in value_dict

    Examples:
        >>> df = set_values(df, {
        ...     "agency": "Brusly PD",
        ...     "data_production_year": 2020
        ... })

    Args:
        df (pd.DataFrame):
            the frame to process
        value_dict (dict):
            the mapping between column name and what value should be set
            for that column.

    Returns:
        the updated frame
    """
    for col, val in value_dict.items():
        df.loc[:, col] = val
    return df
