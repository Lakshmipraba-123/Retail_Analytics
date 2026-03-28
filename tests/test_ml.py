import pandas as pd
import numpy as np
import pytest
from ml_forecast import engineer_features, train_model

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "PRODUCT_ID":           range(20),
        "CATEGORY":             ["Electronics"] * 10 + ["Clothing"] * 10,
        "BRAND":                ["BrandA"] * 20,
        "PRICE_USD":            np.random.uniform(10, 500, 20),
        "PRODUCT_DISCOUNT_PCT": np.random.uniform(0, 40, 20),
        "AVG_RATING":           np.random.uniform(3, 5, 20),
        "STOCK_QUANTITY":       np.random.randint(0, 200, 20),
        "UNITS_SOLD":           np.random.randint(1, 100, 20),
        "LINE_TOTAL_USD":       np.random.uniform(20, 300, 20),
        "ORDER_DISCOUNT_PCT":   np.random.uniform(0, 20, 20),
    })

def test_engineer_features_shape(sample_df):
    X, y, _, _, _ = engineer_features(sample_df)
    assert X.shape[0] == len(sample_df)
    assert X.shape[1] == 18   # updated to match current feature_cols count

def test_engineer_features_no_nulls(sample_df):
    X, y, _, _, _ = engineer_features(sample_df)
    assert not X.isnull().any().any()

def test_model_trains_and_predicts(sample_df):
    X, y, _, _, _ = engineer_features(sample_df)
    model, metrics = train_model(X, y)
    preds = model.predict(X)
    assert len(preds) == len(y)
    assert "mae" in metrics and "r2" in metrics
    assert metrics["r2"] <= 1.0