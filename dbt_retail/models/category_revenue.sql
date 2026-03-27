{{ config(materialized='view') }}

SELECT
    p.category,
    COUNT(DISTINCT o.order_id)            AS total_orders,
    SUM(o.quantity_ordered)               AS units_sold,
    ROUND(SUM(o.line_discounted_usd), 2)  AS total_revenue_usd,
    ROUND(SUM(o.line_savings_usd), 2)     AS total_savings_usd,
    ROUND(AVG(p.avg_rating), 2)           AS avg_rating
FROM {{ source('analytics', 'fact_orders') }} o
JOIN {{ source('analytics', 'dim_products') }} p
  ON o.product_id = p.product_id
GROUP BY p.category