SELECT 
    product_id,
    product,
    warehouse,
    date,
    ordered,
    sold,
    incidents,
    cumulative_remaining_quantity
FROM {{ ref('fct_daily_quantity_events') }}
WHERE product_id IN (
    SELECT product_id 
    FROM {{ ref('stg_daily_demand_base') }}
)
    AND product IS NOT NULL
    AND warehouse IS NOT NULL
    AND date IS NOT NULL
ORDER BY warehouse, product, date

