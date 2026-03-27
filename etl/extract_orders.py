import requests
import pandas as pd
import json
import os
from datetime import datetime

# ── CONFIGURATION ──────────────────────────────────────────
API_BASE_URL       = "https://dummyjson.com"
RAW_DATA_PATH      = "/opt/airflow/data/raw"
PROCESSED_DATA_PATH = "/opt/airflow/data/processed"

# ── EXTRACT ────────────────────────────────────────────────
def extract_orders():
    print("Starting EXTRACT: Fetching all orders (carts)...")
    all_carts = []
    limit     = 100
    skip      = 0

    while True:
        url      = f"{API_BASE_URL}/carts?limit={limit}&skip={skip}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"API failed: {response.status_code}")
        data      = response.json()
        batch     = data["carts"]
        total     = data["total"]

        all_carts.extend(batch)
        skip += limit

        print(f"Fetched {len(all_carts)} / {total} orders...")

        if skip >= total:
            break

    print(f"SUCCESS: Extracted {len(all_carts)} orders")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_file  = os.path.join(RAW_DATA_PATH, f"orders_raw_{timestamp}.json")
    with open(raw_file, "w") as f:
        json.dump(all_carts, f, indent=2)

    print(f"Raw data saved to: {raw_file}")
    return all_carts

# ── TRANSFORM ──────────────────────────────────────────────
def transform_orders(raw_carts):
    print("Starting TRANSFORM: Flattening nested orders data...")
    flat_rows = []

    for cart in raw_carts:

        order_id         = cart["id"]
        user_id          = cart["userId"]
        order_total      = cart["total"]
        order_discounted = cart["discountedTotal"]
        total_products   = cart["totalProducts"]
        total_quantity   = cart["totalQuantity"]
    
        for product in cart["products"]:
            row = {
                # ── Order-level info (repeated for each product) ──
                "order_id":              order_id,
                "user_id":               user_id,
                "order_total_usd":       order_total,
                "order_discounted_usd":  order_discounted,
                "order_total_products":  total_products,
                "order_total_quantity":  total_quantity,

                # ── Product-level info (unique per row) ──
                "product_id":            product["id"],
                "product_name":          product["title"],
                "unit_price_usd":        product["price"],
                "quantity_ordered":      product["quantity"],
                "line_total_usd":        product["total"],
                "discount_pct":          product["discountPercentage"],
                "line_discounted_usd":   product["discountedTotal"],
                }
            flat_rows.append(row)
    df = pd.DataFrame(flat_rows)
    print(f"Rows after flattening: {len(df)}")
    print(f"  (this means {len(df)} individual product lines across all orders)")

    # ── Fix data types ──
    numeric_cols = [
        "order_total_usd", "order_discounted_usd",
        "unit_price_usd",  "quantity_ordered",
        "line_total_usd",  "discount_pct",
        "line_discounted_usd"
    ]

    for col in numeric_cols:
        df[col] =pd.to_numeric(df[col],errors="coerce").fillna(0)

    money_cols = [
        "order_total_usd", "order_discounted_usd",
        "unit_price_usd",  "line_total_usd",
        "line_discounted_usd"
    ]
    for col in money_cols:
        df[col] = df[col].round(2)

    #  ── Add columns ──
    df["line_savings_usd"] = (df["line_total_usd"] - df["line_discounted_usd"]).round(2)

    # ── Add processed timestamp ──
    df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Columns in clean data: {list(df.columns)}")
    return df

# ── LOAD ───────────────────────────────────────────────────
def load_orders(clean_df):
    print("Starting LOAD: Saving clean orders to CSV...")
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(PROCESSED_DATA_PATH, f"orders_clean_{timestamp}.csv")

    clean_df.to_csv(output_file, index=False)

    print(f"SUCCESS: Saved to {output_file}")
    print(f"Final dataset: {len(clean_df)} order lines, {len(clean_df.columns)} columns")
    return output_file

# ── MAIN PIPELINE ──────────────────────────────────────────
def run_orders_etl():
    print("=" * 50)
    print("STARTING ORDERS ETL PIPELINE")
    print("=" * 50)

    raw_data   = extract_orders()
    clean_data = transform_orders(raw_data)
    output     = load_orders(clean_data)

    print("=" * 50)
    print("ORDERS ETL COMPLETED SUCCESSFULLY")
    print(f"Output: {output}")
    print("=" * 50)


if __name__ == "__main__":
    run_orders_etl()

