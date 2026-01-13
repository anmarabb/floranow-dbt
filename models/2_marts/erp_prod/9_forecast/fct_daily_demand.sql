SELECT 
    qe.product_id,
    base.product,
    qe.warehouse,
    qe.date,
    qe.ordered,
    qe.sold,
    qe.incidents,
    qe.cumulative_remaining_quantity,
    base.age,
    base.product_category
FROM {{ ref('fct_daily_quantity_events') }} qe
INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product_id = qe.product_id
WHERE qe.product IS NOT NULL
    AND qe.warehouse IS NOT NULL
    AND qe.date IS NOT NULL
ORDER BY qe.warehouse, base.product, qe.date

