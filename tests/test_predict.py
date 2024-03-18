import pandas as pd
import pytest
from sklearn.base import BaseEstimator, TransformerMixin

from categorize_transactions.predict import load_object, predict_and_save


# Mocking the transformer and model classes
class MockTransformer(BaseEstimator, TransformerMixin):
    def transform(self, X):
        return X


class MockModel(BaseEstimator):
    feature_names_in_ = ['feature1', 'feature2']

    def predict(self, X):
        return [1] * len(X)


@pytest.fixture
def mock_df():
    """Create a mock DataFrame for testing."""
    return pd.DataFrame({'feature1': [0.1, 0.2, 0.3], 'feature2': [1, 2, 3]})


@pytest.fixture
def mock_transformer():
    """Provide a mock transformer."""
    return MockTransformer()


@pytest.fixture
def mock_model():
    """Provide a mock model."""
    return MockModel()


def test_load_object_local(mocker):
    """Test loading an object from the local filesystem."""
    mock_open = mocker.mock_open(read_data='mock data')
    mocker.patch('builtins.open', mock_open)
    mocker.patch('dill.loads', return_value='mock object')

    result = load_object('local', 'mock_bucket', 'mock_key')

    assert result == 'mock object', "Failed to load the mock object correctly."


def test_predict_and_save_local(mocker, mock_df, mock_transformer, mock_model):
    """Test the predict and save functionality for local storage."""
    mock_save_df = mocker.patch('categorize_transactions.predict.save_df')

    predict_and_save(mock_df, mock_transformer, mock_model, 'local', 'mock_bucket', 'mock_output_key')

    mock_save_df.assert_called_once()
    assert 'predictions' in mock_df.columns, "Predictions column not added to DataFrame."
