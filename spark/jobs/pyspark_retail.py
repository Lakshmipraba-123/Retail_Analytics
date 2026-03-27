# ============================================================
# Phase 2 — PySpark Retail Analytics Job
# File: spark/jobs/pyspark_retail.py
# ============================================================

import os
import glob

# Fix Java and Hadoop warnings before importing PySpark
os.environ['JAVA_HOME']      = r'C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── START SPARK ────────────────────────────────────────────
print("=" * 55)
print("PHASE 2 — PYSPARK RETAIL ANALYTICS")
print("=" * 55)

spark = SparkSession.builder \
    .appName("Retail Analytics — Phase 2") \
    .master("local[*]") \
    .config("spark.ui.enabled",            "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✓ Spark started successfully\n")

# ── LOAD DATA ──────────────────────────────────────────────
# Find the latest products and orders CSV files
# from Phase 1 output
PROCESSED_PATH = r"C:\retail_analytics\data\processed"

products_files = glob.glob(f"{PROCESSED_PATH}\\products_clean_*.csv")
orders_files   = glob.glob(f"{PROCESSED_PATH}\\orders_clean_*.csv")

# Pick the most recently created file
latest_products = max(products_files)
latest_orders   = max(orders_files)

print(f"✓ Loading products from: {latest_products}")
print(f"✓ Loading orders from:   {latest_orders}\n")

products_df = spark.read.csv(latest_products, header=True, inferSchema=True)
orders_df   = spark.read.csv(latest_orders,   header=True, inferSchema=True)

print(f"✓ Products loaded: {products_df.count()} rows")
print(f"✓ Orders loaded:   {orders_df.count()} rows\n")

# ── TRANSFORMATION 1: Revenue by category ─────────────────
# Business question: which category makes the most money?
print("=" * 55)
print("ANALYSIS 1 — REVENUE BY CATEGORY")
print("=" * 55)

revenue_by_category = products_df \
    .groupBy("category") \
    .agg(
        F.count("product_id")
         .alias("total_products"),
        F.round(F.sum("actual_price_usd"), 2)
         .alias("total_stock_value_usd"),
        F.round(F.avg("actual_price_usd"), 2)
         .alias("avg_price_usd"),
        F.round(F.avg("avg_rating"), 2)
         .alias("avg_rating"),
        F.sum("stock_quantity")
         .alias("total_units_in_stock")
    ) \
    .orderBy(F.desc("total_stock_value_usd"))

revenue_by_category.show(truncate=False)

# ── TRANSFORMATION 2: Dead inventory ──────────────────────
# Business question: which products are stuck in warehouse?
print("=" * 55)
print("ANALYSIS 2 — DEAD INVENTORY ALERT")
print("=" * 55)

dead_inventory = products_df \
    .filter(
        (F.col("stock_quantity") > 50) &
        (F.col("avg_rating") < 3.5)
    ) \
    .select(
        "product_name",
        "category",
        "stock_quantity",
        "avg_rating",
        "actual_price_usd"
    ) \
    .withColumn(
        "stock_value_at_risk_usd",
        F.round(
            F.col("stock_quantity") * F.col("actual_price_usd"), 2
        )
    ) \
    .orderBy(F.desc("stock_value_at_risk_usd"))

dead_inventory.show(truncate=False)
print(f"Total at-risk products: {dead_inventory.count()}\n")

# ── TRANSFORMATION 3: Discount impact ─────────────────────
# Business question: how much revenue are discounts costing?
print("=" * 55)
print("ANALYSIS 3 — DISCOUNT IMPACT BY CATEGORY")
print("=" * 55)

discount_impact = products_df \
    .groupBy("category") \
    .agg(
        F.round(F.avg("discount_pct"), 2)
         .alias("avg_discount_pct"),
        F.round(
            F.sum("price_usd") - F.sum("actual_price_usd"), 2
        ).alias("total_revenue_lost_usd"),
        F.count("product_id")
         .alias("products_discounted")
    ) \
    .orderBy(F.desc("total_revenue_lost_usd"))

discount_impact.show(truncate=False)

# ── TRANSFORMATION 4: Join orders with products ───────────
# Business question: what is the revenue per order category?
# This joins two datasets — a core PySpark skill
print("=" * 55)
print("ANALYSIS 4 — ORDER REVENUE BY PRODUCT CATEGORY")
print("=" * 55)

order_analysis = orders_df \
    .join(
        products_df.select("product_id", "category", "brand"),
        on="product_id",
        how="left"
    ) \
    .groupBy("category") \
    .agg(
        F.count("order_id")
         .alias("total_order_lines"),
        F.round(F.sum("line_discounted_usd"), 2)
         .alias("total_revenue_usd"),
        F.round(F.avg("quantity_ordered"), 2)
         .alias("avg_qty_per_order"),
        F.round(F.sum("line_savings_usd"), 2)
         .alias("total_customer_savings_usd")
    ) \
    .orderBy(F.desc("total_revenue_usd"))

order_analysis.show(truncate=False)

# ── SAVE ALL RESULTS ───────────────────────────────────────
print("=" * 55)
print("SAVING RESULTS")
print("=" * 55)

OUTPUT_PATH = r"C:\retail_analytics\spark\output"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Save each analysis as CSV for Phase 3 (Snowflake)
revenue_by_category.toPandas().to_csv(
    f"{OUTPUT_PATH}\\revenue_by_category.csv", index=False
)
dead_inventory.toPandas().to_csv(
    f"{OUTPUT_PATH}\\dead_inventory.csv", index=False
)
discount_impact.toPandas().to_csv(
    f"{OUTPUT_PATH}\\discount_impact.csv", index=False
)
order_analysis.toPandas().to_csv(
    f"{OUTPUT_PATH}\\order_analysis.csv", index=False
)

print(f"✓ revenue_by_category.csv saved")
print(f"✓ dead_inventory.csv saved")
print(f"✓ discount_impact.csv saved")
print(f"✓ order_analysis.csv saved")

spark.stop()

print("\n" + "=" * 55)
print("PHASE 2 COMPLETE")
print("4 business analyses produced and saved")
print("Ready for Phase 3 — Snowflake data warehouse")
print("=" * 55)