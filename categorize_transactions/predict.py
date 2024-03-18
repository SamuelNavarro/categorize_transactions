"""Predicts on new data and saves the results."""

import logging
from pathlib import Path

import boto3
import dill
from configs.utils import read_and_clean_csv, save_df

BASEDIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASEDIR / "artifacts"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_object(storage_type, bucket, key):
    """Load a Python object from S3 or locally.

    Parameters:
    -----------
    storage_type : str
        's3' for Amazon S3 or 'local' for local filesystem.
    bucket : str
        The S3 bucket name.
    key : str
        The S3 key for the object.

    Returns:
    --------
    object
        The Python object loaded from S3.
    """
    if storage_type == 's3':
        s3_resource = boto3.resource('s3')
        obj = s3_resource.Object(bucket, key)
        data = obj.get()['Body'].read()
    else:
        file_path = ARTIFACTS_DIR / bucket / key
        with open(file_path, 'rb') as f:
            data = f.read()

    return dill.loads(data)


def predict_and_save(df, transformer, model, storage_type, bucket, output_key):
    """
    Applies transformations, makes predictions, and saves the resulting DataFrame.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to predict on.
    transformer : The transformation pipeline.
    model : The trained model.
    storage_type : str
        's3' for Amazon S3 or 'local' for local filesystem.
    bucket : str
        The S3 bucket name.
    output_key : str
        The S3 key for the output file.
    """
    # Transform the features for prediction
    transformed_features = transformer.transform(df)

    # Make predictions on the transformed features
    predictions = model.predict(transformed_features[model.feature_names_in_])

    # Append the predictions to the original DataFrame
    df['predictions'] = predictions

    # Save to S3 as Parquet
    save_df(df, bucket, output_key, storage_type=storage_type, file_format='parquet')
    logging.info(f"Predictions saved to {bucket}/{output_key} in {storage_type} storage.")


def main():
    storage_type = 'local'
    bucket = 'brazilian-credit'
    transformer_key = 'transformations/transformers.pkl'
    model_key = 'trained_model/model.pkl'
    input_key = 'MiBolsillo.csv'
    output_key = 'output/predictions.parquet'

    # Load transformer and model
    transformer = load_object(storage_type, bucket, transformer_key)
    model = load_object(storage_type, bucket, model_key)

    # Read, clean, and preprocess data
    file_path = (ARTIFACTS_DIR / input_key) if storage_type == 'local' else f's3://{bucket}/{input_key}'
    df = read_and_clean_csv(file_path)

    # Predict and save results
    predict_and_save(df, transformer, model, storage_type, bucket, output_key)


if __name__ == "__main__":
    main()
