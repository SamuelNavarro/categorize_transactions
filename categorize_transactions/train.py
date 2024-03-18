import logging
from pathlib import Path

import pandas as pd
from configs.utils import save_artifact
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

BASEDIR = Path(__file__).resolve().parents[1]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_preprocessed_data(s3_path, file_format='parquet'):
    """
    Loads preprocessed data from an S3 path.

    Parameters
    ----------
    s3_path : str
        The S3 path to the file.
    file_format : str, default 'parquet'
        The format of the file to load ('csv' or 'parquet').

    Returns
    -------
    df : pandas.DataFrame
        The loaded preprocessed dataset.
    """
    logging.info(f"Loading preprocessed data from {s3_path}")
    if file_format == 'csv':
        df = pd.read_csv(s3_path)
    elif file_format == 'parquet':
        df = pd.read_parquet(s3_path, engine='pyarrow')
    else:
        raise ValueError("Unsupported file format. Please use 'csv' or 'parquet'.")
    return df


def train_and_save_model(df, storage_type, target_column, bucket, model_key, ignore_columns=[]):
    """
    Trains a RandomForestClassifier and saves the model to S3.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing the features and target.
    target_column : str
        The name of the target variable column.
    bucket : str
        The name of the S3 bucket to save the model.
    model_key : str
        The S3 key for the saved model.
    ignore_columns : list of str
        The list of column names to ignore during training.
    """
    logging.info("Training the model...")
    # Exclude ignored columns and the target column from the features
    X = df.drop(ignore_columns + [target_column], axis=1)
    y = df[target_column]

    # Splitting the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train a RandomForestClassifier
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    save_artifact(model, storage_type, bucket, model_key)


def main():
    bucket = "brazilian-credit"
    model_key = "trained_model/model.pkl"
    storage_type = "local"
    target_column = "grupo_estabelecimento"
    ignore_columns = ["id", "safra_abertura", "data"]

    ROOT_DATA_PATH = BASEDIR / "artifacts" / f"{bucket}"

    if storage_type == "s3":
        file_path = f"s3://{bucket}/input_model/transformed_MiBolsillo.parquet"
    else:
        file_path = ROOT_DATA_PATH / "input_model" / "transformed_MiBolsillo.parquet"

    # Load the preprocessed data
    df = load_preprocessed_data(file_path, file_format='parquet')

    # Train the model and save to S3
    train_and_save_model(df, storage_type, target_column, bucket, model_key, ignore_columns)


if __name__ == "__main__":
    main()
