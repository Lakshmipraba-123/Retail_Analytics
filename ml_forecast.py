import os
import json
import pickle
import pandas as pd
import numpy as np
from datetime import date
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, r2_score

# ── 1. CONNECTION ──────────────────────────────────────────────────────────────
def get_snowflake_conn():
    return connect(
        account   = "jo29319.ap-southeast-1",
        user      = "LIPA2312",        # replace with your actual username
        password  = "K2iMAvwG42AKZVW",        # replace with your actual password
        warehouse = "RETAIL_WH",
        database  = "RETAIL_ANALYTICS",
        schema    = "ANALYTICS",
    )

# ── 2. EXTRACT ─────────────────────────────────────────────────────────────────
def extract_training_data(conn) -> pd.DataFrame:
    query = """
        SELECT
            f.ORDER_ID,
            f.PRODUCT_ID,
            f.QUANTITY_ORDERED        AS UNITS_SOLD,
            f.LINE_TOTAL_USD,
            f.DISCOUNT_PCT            AS ORDER_DISCOUNT_PCT,
            p.CATEGORY,
            p.BRAND,
            p.AVG_RATING,
            p.STOCK_QUANTITY,
            p.PRICE_USD,
            p.DISCOUNT_PCT            AS PRODUCT_DISCOUNT_PCT
        FROM RETAIL_ANALYTICS.ANALYTICS.FACT_ORDERS f
        JOIN RETAIL_ANALYTICS.ANALYTICS.DIM_PRODUCTS p
          ON f.PRODUCT_ID = p.PRODUCT_ID
    """
    cursor = conn.cursor()
    cursor.execute(query)
    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)

# ── 3. FEATURE ENGINEERING ────────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame):
    df = df.copy()

    le_cat   = LabelEncoder()
    le_brand = LabelEncoder()
    df["CATEGORY_ENC"]          = le_cat.fit_transform(df["CATEGORY"].fillna("unknown"))
    df["BRAND_ENC"]             = le_brand.fit_transform(df["BRAND"].fillna("unknown"))

    # Use PRODUCT_DISCOUNT_PCT (from DIM_PRODUCTS)
    df["PRICE_LOG"]             = np.log1p(df["PRICE_USD"])
    df["PRICE_AFTER_DISC"]      = df["PRICE_USD"] * (1 - df["PRODUCT_DISCOUNT_PCT"] / 100)
    df["DISCOUNT_RATE"]         = df["PRODUCT_DISCOUNT_PCT"] / 100
    df["IS_DISCOUNTED"]         = (df["PRODUCT_DISCOUNT_PCT"] > 0).astype(int)
    df["PRICE_TIER"]            = pd.qcut(df["PRICE_USD"], q=4,
                                          labels=[0,1,2,3], duplicates="drop").astype(int)

    # Stock
    df["STOCK_LOG"]             = np.log1p(df["STOCK_QUANTITY"])
    df["IS_OUT_OF_STOCK"]       = (df["STOCK_QUANTITY"] == 0).astype(int)
    df["IS_LOW_STOCK"]          = (df["STOCK_QUANTITY"] < 10).astype(int)
    df["STOCK_TO_SALES_RATIO"]  = df["STOCK_QUANTITY"] / (df["UNITS_SOLD"] + 1)

    # Rating
    df["IS_HIGH_RATED"]         = (df["AVG_RATING"] >= 4.5).astype(int)
    df["RATING_TIMES_DISCOUNT"] = df["AVG_RATING"] * df["DISCOUNT_RATE"]

    # Aggregations
    cat_avg = df.groupby("CATEGORY_ENC")["UNITS_SOLD"].transform("mean")
    cat_std = df.groupby("CATEGORY_ENC")["UNITS_SOLD"].transform("std").fillna(1)
    df["CATEGORY_AVG_SALES"]    = cat_avg
    df["CATEGORY_SALES_Z"]      = (df["UNITS_SOLD"] - cat_avg) / cat_std
    df["BRAND_AVG_SALES"]       = df.groupby("BRAND_ENC")["UNITS_SOLD"].transform("mean")

    feature_cols = [
        "CATEGORY_ENC", "BRAND_ENC",
        "PRICE_USD", "PRICE_LOG", "PRICE_AFTER_DISC", "PRICE_TIER",
        "DISCOUNT_RATE", "IS_DISCOUNTED",
        "AVG_RATING", "IS_HIGH_RATED", "RATING_TIMES_DISCOUNT",
        "STOCK_LOG", "IS_OUT_OF_STOCK", "IS_LOW_STOCK", "STOCK_TO_SALES_RATIO",
        "CATEGORY_AVG_SALES", "CATEGORY_SALES_Z", "BRAND_AVG_SALES",
    ]

    X = df[feature_cols]
    y = df["UNITS_SOLD"]
    return X, y, df, le_cat, le_brand

# ── 4. TRAIN ──────────────────────────────────────────────────────────────────
def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # Evaluation
    y_pred = model.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    r2     = r2_score(y_test, y_pred)
    cv_r2  = cross_val_score(model, X, y, cv=5, scoring="r2").mean()

    print(f"MAE: {mae:.2f}  |  R²: {r2:.3f}  |  CV R² (5-fold): {cv_r2:.3f}")
    return model, {"mae": round(mae, 4), "r2": round(r2, 4), "cv_r2": round(cv_r2, 4)}

# ── 5. WRITE PREDICTIONS BACK ─────────────────────────────────────────────────
def write_predictions(conn, model, df, X, metrics):
    df = df.copy()
    df["PREDICTED_UNITS"] = model.predict(X).round(2)
    df["RUN_DATE"]        = date.today().isoformat()
    df["MAE"]             = metrics["mae"]
    df["R2_SCORE"]        = metrics["r2"]

    out = df[[
        "PRODUCT_ID", "CATEGORY", "PREDICTED_UNITS",
        "UNITS_SOLD", "RUN_DATE", "MAE", "R2_SCORE",
    ]].rename(columns={"UNITS_SOLD": "ACTUAL_UNITS"})

    conn.cursor().execute("TRUNCATE TABLE IF EXISTS RETAIL_ANALYTICS.ANALYTICS.ML_PREDICTIONS")
    success, nchunks, nrows, _ = write_pandas(conn, out, "ML_PREDICTIONS")
    print(f"Written {nrows} rows to ML_PREDICTIONS in {nchunks} chunk(s).")

# ── 6. PERSIST ARTEFACTS ──────────────────────────────────────────────────────
def save_artifacts(model, metrics):
    with open("model.pkl", "wb") as f:
        pickle.dump(model, f)
    with open("metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print("Saved model.pkl and metrics.json")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    conn = get_snowflake_conn()
    try:
        df            = extract_training_data(conn)
        X, y, df, *_ = engineer_features(df)
        model, metrics = train_model(X, y)
        write_predictions(conn, model, df, X, metrics)
        save_artifacts(model, metrics)
    finally:
        conn.close()