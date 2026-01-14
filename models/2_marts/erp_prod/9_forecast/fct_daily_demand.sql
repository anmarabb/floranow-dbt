WITH daily_demand_data AS (
    SELECT 
        qe.warehouse,
        base.product,
        qe.date,
        SUM(qe.ordered) AS ordered,
        SUM(qe.sold) AS sold,
        SUM(qe.incidents) AS incidents,
        SUM(qe.warehoused) AS warehoused,
        SUM(qe.cumulative_remaining_quantity) AS cumulative_remaining_quantity,
        base.age,
        base.product_category
    FROM {{ ref('fct_daily_quantity_events') }} qe
    INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product_id = qe.product_id AND base.warehouse = qe.warehouse
    GROUP BY qe.warehouse, base.product, qe.date, base.age, base.product_category
)

SELECT 
    ac.date,
    ac.warehouse,
    ac.product,
    SUM(COALESCE(orig.ordered, 0)) AS ordered,
    SUM(COALESCE(orig.sold, 0)) AS sold,
    SUM(COALESCE(orig.incidents, 0)) AS incidents,
    SUM(COALESCE(orig.warehoused, 0)) AS warehoused,
    SUM(COALESCE(orig.cumulative_remaining_quantity, 0)) AS cumulative_remaining_quantity,
    MAX(COALESCE(orig.age, ac.age, 0)) AS age,
    MAX(COALESCE(orig.product_category, ac.product_category, '')) AS product_category
FROM {{ ref('int_daily_demand_combinations') }} ac
LEFT JOIN daily_demand_data orig
    ON ac.date = orig.date
    AND ac.warehouse = orig.warehouse
    AND ac.product = orig.product
GROUP BY ac.date, ac.warehouse, ac.product
ORDER BY ac.warehouse, ac.product, ac.date

