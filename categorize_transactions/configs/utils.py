"""Utility functions for saving artifacts and reading CSV files from S3."""

import logging
from io import BytesIO, StringIO
from pathlib import Path

import boto3
import dill
import pandas as pd

from .schema import data_schema

s3_resource = boto3.resource("s3")

BASEDIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASEDIR / "artifacts"


def save_artifact(model, storage_type, bucket, model_key):
    """Save a model to S3 or locally."""
    serialized_model = BytesIO()
    dill.dump(model, serialized_model)
    serialized_model.seek(0)

    if storage_type == "s3":
        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, model_key).put(Body=serialized_model.getvalue())
        logging.info(f"Artifact saved to S3 at {bucket}/{model_key}")
    else:
        local_path = ARTIFACTS_DIR / bucket / model_key
        local_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory structure if doesn't exist
        with open(local_path, 'wb') as f:
            f.write(serialized_model.getvalue())
        logging.info(f"Artifact saved locally at {local_path}")


def save_df(df, bucket=None, key=None, storage_type='s3', file_format='parquet'):
    """Save a pandas DataFrame as a CSV or Parquet file either in an S3 bucket or locally within an 'artifacts' folder.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to save.
    storage_type : str, optional
        The type of storage to use ('s3' or 'local'), default to 's3'.
    bucket : str, optional
        The name of the S3 bucket. Required if storage_type is 's3'.
    key : str, optional
        The key for the saved file in the S3 bucket or the local file path within the 'artifacts' folder.
    file_format : str, optional
        The format of the file to save ('csv' or 'parquet'), default to 'parquet'.
    """
    if storage_type == 's3':
        if not bucket or not key:
            raise ValueError("Bucket and key are required for S3 storage.")

        s3_resource = boto3.resource('s3')
        if file_format == 'csv':
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
        elif file_format == 'parquet':
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            s3_resource.Object(bucket, key).put(Body=parquet_buffer.getvalue())
        else:
            raise ValueError("Unsupported file format. Use 'csv' or 'parquet'.")
    elif storage_type == 'local':
        local_path = BASEDIR / 'artifacts' / bucket / key
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if file_format == 'csv':
            df.to_csv(local_path, index=False)
        elif file_format == 'parquet':
            df.to_parquet(local_path, index=False)
        else:
            raise ValueError("Unsupported file format. Use 'csv' or 'parquet'.")
    else:
        raise ValueError("Unsupported storage type. Use 's3' or 'local'.")


def read_and_clean_csv(path):
    """
    Reads a CSV file from an S3 path and returns it as a pandas DataFrame.

    Parameters
    ----------
    path : str
        The path to the file (e.g., 's3://bucket-name/path/to/file.csv', 'path/to/file.csv').

    Returns
    -------
    pandas.DataFrame
        The data from the CSV file as a pandas DataFrame.
    """
    try:
        df = pd.read_csv(path, delimiter=";", dtype=data_schema)
    except UnicodeDecodeError:
        df = pd.read_csv(
            path,
            delimiter=";",
            encoding="latin-1",
            low_memory=True,
            decimal=',',
            na_values=' -   ',
            dtype=data_schema,
            thousands='.',
        )

    df['data'] = pd.to_datetime(df['data'], dayfirst=True)

    customers = pd.DataFrame(df.id.unique())
    customers.columns = ['old_customer_name']
    new_list = list(range(1, len(customers) + 1))
    customers['new_customer_name'] = new_list

    df.loc[:, "id"] = df["id"].replace(customers['old_customer_name'].values, customers['new_customer_name'].values)

    df.columns = df.columns.str.replace(' ', '')

    df = df.drop_duplicates()

    return df
