
import sys
import os
import pytest
import pandas as pd

# Add the etl folder to path so we can import our scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'etl'))

from extract_products import transform_products


class TestProductsTransform:


    def test_columns_exist_after_transform(self):

        fake_products = [
            {
                "id": 1, "title": "Test Phone", "category": "smartphones",
                "price": 100.0, "discountPercentage": 10.0,
                "rating": 4.5, "stock": 50, "brand": "TestBrand"
            }
        ]
        result = transform_products(fake_products)
        expected_columns = [
            "product_id", "product_name", "category",
            "price_usd", "discount_pct", "avg_rating",
            "stock_quantity", "brand", "actual_price_usd"
        ]
        for col in expected_columns:
            assert col in result.columns, f"Missing column: {col}"

    def test_actual_price_calculated_correctly(self):

        fake_products = [
            {
                "id": 1, "title": "Test Phone", "category": "smartphones",
                "price": 100.0, "discountPercentage": 10.0,
                "rating": 4.5, "stock": 50, "brand": "TestBrand"
            }
        ]
        result = transform_products(fake_products)
        assert result["actual_price_usd"].iloc[0] == 90.0

    def test_missing_brand_filled_with_unknown(self):

        fake_products = [
            {
                "id": 1, "title": "No Brand Product",
                "category": "misc", "price": 50.0,
                "discountPercentage": 0.0, "rating": 3.0,
                "stock": 10, "brand": None
            }
        ]
        result = transform_products(fake_products)
        assert result["brand"].iloc[0] == "Unknown"

    def test_no_duplicate_products(self):

        fake_products = [
            {
                "id": 1, "title": "Phone", "category": "smartphones",
                "price": 100.0, "discountPercentage": 10.0,
                "rating": 4.5, "stock": 50, "brand": "Brand"
            },
            {
                "id": 1, "title": "Phone", "category": "smartphones",
                "price": 100.0, "discountPercentage": 10.0,
                "rating": 4.5, "stock": 50, "brand": "Brand"
            }
        ]
        result = transform_products(fake_products)
        assert len(result) == 1, "Duplicates were not removed"

    def test_empty_input_returns_empty_dataframe(self):
  
        result = transform_products([])
        assert len(result) == 0


