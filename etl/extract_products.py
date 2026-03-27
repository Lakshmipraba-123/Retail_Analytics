# ============================================================
# Phase 1 - ETL Script: Extract Products from REST API
# File: etl/extract_products.py
# ============================================================
import requests #for calling the rest API
import pandas as pd #for cleaning and shaping data
import json   # for reading JSON responses
import os  # for working with file paths
from datetime import datetime  # for timestamping our files

# ── CONFIGURATION ──────────────────────────────────────────
# The address of the data source
API_BASE_URL = "https://dummyjson.com"

# we save the raw JSON files (before cleaning)
RAW_DATA_PATH = "/opt/airflow/data/raw"

# we save the cleaned CSV files
PROCESSED_DATA_PATH = "/opt/airflow/data/processed"

# ── EXTRACT ────────────────────────────────────────────────
def extract_products():
    print("Starting EXTRACT: Calling DummyJSON API for products...")

    limit = 100
    skip = 0
    all_products = []

    while True:
        # Build URL with pagination
        url = f"{API_BASE_URL}/products?limit={limit}&skip={skip}"
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"API call failed with status code: {response.status_code}")

        data = response.json()
        batch = data["products"]
        total = data["total"]

        all_products.extend(batch)
        skip += limit

        print(f"Fetched {len(all_products)} / {total} products...")

        if skip >= total:
            break

    print(f"SUCCESS: Extracted {len(all_products)} total products")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        #keeping the raw data untouched
    raw_file= os.path.join(RAW_DATA_PATH, f"products_raw_{timestamp}.json")

    with open(raw_file,"w") as f:
        json.dump(all_products, f, indent=2)
    print(f"Raw data saved to: {raw_file}")
    return all_products #pass the data to the next steps


# ── TRANSFORM ──────────────────────────────────────────────
def transform_products(raw_products):
    print("Starting TRANSFORM: Cleaning product data with pandas...")
    print(f"Columns from API:{[]if not raw_products else list(raw_products[0].keys())}")
    if not raw_products:
        print("WARNING: Empty product list received — skipping transform")
        return pd.DataFrame()
    print(f"Total rows before cleaning: {len(raw_products)}")
    df = pd.DataFrame(raw_products)
    print(f"Columns from API:{list(df.columns)}")
    print(f"Total rows before cleaning {len(df)}")

    columns_to_keep = [
        "id",
        "title",
        "category",
        "price",
        "discountPercentage",
        "rating",
        "stock",
        "brand",
    ]

    df = df[columns_to_keep]

    # ── Rename columns to be clean and consistent ──
    df = df.rename(columns = {
        "id": "product_id",
        "title": "product_name",
        "category": "category",
        "price": "price_usd",
        "discountPercentage": "discount_pct",
        "rating": "avg_rating",
        "stock": "stock_quantity",
        "brand": "brand"
    })

    ## ── Fixing data types ──
    df["price_usd"]      = pd.to_numeric(df["price_usd"],      errors="coerce")
    df["discount_pct"]   = pd.to_numeric(df["discount_pct"],   errors="coerce")
    df["avg_rating"]     = pd.to_numeric(df["avg_rating"],      errors="coerce")
    df["stock_quantity"] = pd.to_numeric(df["stock_quantity"],  errors="coerce")

    # ── Handle missing values ─
    df["price_usd"] = df["price_usd"].fillna(0)
    df["discount_pct"] = df["discount_pct"].fillna(0)
    df["stock_quantity"] = df["stock_quantity"].fillna(0)
    df["brand"] = df["brand"].fillna("Unknown")
    
    # ___ Adding new column ____
    df["actual_price_usd"] = df["price_usd"] * (1- df["discount_pct"]/100)
    df["actual_price_usd"] = df["actual_price_usd"].round(2)

    df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Remove duplicate rows if any ──
    df = df.drop_duplicates(subset = ["product_id"])

    print(f"Total rows after cleaning: {len(df)}")
    print(f"Columns in clean data: {list(df.columns)}")

    return df 

# ── LOAD ───────────────────────────────────────────────────
def load_products(clean_df):
    print("Starting LOAD: Saving clean data to CSV...")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(PROCESSED_DATA_PATH, f"products_clean_{timestamp}.csv")

    # Save as CSV
    clean_df.to_csv(output_file, index=False)

    print(f"SUCCESS: Clean data saved to {output_file}")
    print(f"Final dataset: {len(clean_df)} products, {len(clean_df.columns)} columns")

    return output_file

# ── MAIN PIPELINE ──────────────────────────────────────────
def run_products_etl():
    #Extract → Transform → Load

    print("=" * 50)
    print("STARTING PRODUCTS ETL PIPELINE")
    print("=" * 50)

    raw_data = extract_products()
    clean_data = transform_products(raw_data)
    output = load_products(clean_data)

    print("=" * 50)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Output file: {output}")
    print("=" * 50)

if __name__ == "__main__":
    run_products_etl()




