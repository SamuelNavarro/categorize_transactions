"""Data pulling, cleaning and perform a couple of common transformations."""

import logging
from pathlib import Path

import boto3
from configs.utils import read_and_clean_csv, save_artifact, save_df
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

s3_resource = boto3.resource("s3")


BASEDIR = Path(__file__).resolve().parents[1]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def transform_and_save_transformer(df, bucket, transformer_key, storage_type='s3'):
    """Ensure a consistent output format and manage feature names."""
    # Identifying numerical and categorical features
    numerical_features = ['idade', 'limite_total', 'limite_disp', 'valor']
    categorical_features = ['cidade', 'estado', 'sexo', 'cidade_estabelecimento', 'pais_estabelecimento']

    # Defining transformers
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numerical_features),
            ('cat', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=False), categorical_features),
        ],
        remainder='passthrough',
    )

    preprocessor.set_output(transform="pandas")

    # Applying transformations
    transformed_data = preprocessor.fit_transform(df)

    transformed_data.columns = [col.replace('remainder__', '') for col in transformed_data.columns]

    # Serialize and save the preprocessor object to S3
    save_artifact(preprocessor, storage_type, bucket, transformer_key)

    return transformed_data


def main():
    """Execute the data pull, cleaning, transformation, and saving transformer process."""
    bucket = 'brazilian-credit'
    input_key = 'MiBolsillo.csv'
    output_key = 'input_model/transformed_MiBolsillo.parquet'
    transformer_key = 'transformations/transformers.pkl'
    storage_type = 'local'

    if storage_type == 's3':
        file_path = f's3://{bucket}/{input_key}'
    else:
        file_path = BASEDIR / "artifacts" / "MiBolsillo.csv"

    # Read data from S3
    df = read_and_clean_csv(file_path)

    # Transform data and save the transformer
    transformed_df = transform_and_save_transformer(df, bucket, transformer_key, storage_type=storage_type)

    # Save transformed data back to S3
    save_df(transformed_df, bucket, output_key, storage_type=storage_type)


if __name__ == '__main__':
    main()
