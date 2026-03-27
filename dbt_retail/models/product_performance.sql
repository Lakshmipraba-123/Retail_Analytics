{{ config(materialized='view') }}

SELECT
    p.product_name,
    p.category,
    p.brand,
    p.avg_rating,
    p.stock_quantity,
    p.actual_price_usd,
    COALESCE(SUM(o.quantity_ordered), 0)              AS units_sold,
    ROUND(COALESCE(SUM(o.line_discounted_usd), 0), 2) AS revenue_usd
FROM {{ source('analytics', 'dim_products') }} p
LEFT JOIN {{ source('analytics', 'fact_orders') }} o
       ON p.product_id = o.product_id
GROUP BY
    p.product_name, p.category, p.brand,
    p.avg_rating, p.stock_quantity, p.actual_price_usd