{{ config(materialized='view') }}

SELECT
    product_id,
    category,
    predicted_units,
    actual_units,
    run_date,
    mae,
    r2_score,
    ROUND(predicted_units - actual_units, 2)          AS prediction_error,
    ROUND((predicted_units - actual_units)
          / NULLIF(actual_units, 0) * 100, 1)         AS pct_error,
    CASE
        WHEN ABS(predicted_units - actual_units) <= 5 THEN 'excellent'
        WHEN ABS(predicted_units - actual_units) <= 15 THEN 'good'
        ELSE 'needs_review'
    END                                               AS accuracy_band
FROM {{ source('analytics', 'ml_predictions') }}