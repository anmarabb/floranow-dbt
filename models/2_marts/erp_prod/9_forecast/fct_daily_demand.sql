WITH product_attributes AS (
    SELECT 
        product,
        MAX(taxon_age) AS age,
        ARRAY_AGG(product_category IGNORE NULLS ORDER BY CASE WHEN product_category IN ('Others', 'Other') THEN 1 ELSE 0 END, product_category LIMIT 1)[OFFSET(0)] AS product_category
    FROM {{ ref('stg_daily_demand_base') }}
    WHERE product IS NOT NULL
    GROUP BY product
)

SELECT 
    qe.warehouse,
    base.product,
    qe.date,
    SUM(qe.ordered) AS ordered,
    SUM(qe.sold) AS sold,
    SUM(qe.incidents) AS incidents,
    SUM(qe.warehoused) AS warehoused,
    SUM(qe.cumulative_remaining_quantity) AS cumulative_remaining_quantity,
    pa.age,
    COALESCE(
        pa.product_category,
        CASE
            WHEN base.product LIKE '%Rose%' THEN 'Roses'
            WHEN base.product IN ('Ruscus Hypophyllum (large Leaf)', 'Pistacia Sp. (leaf)') THEN 'Greeneries'
            ELSE NULL
        END
    ) AS product_category
FROM {{ ref('fct_daily_quantity_events') }} qe
INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product_id = qe.product_id
LEFT JOIN product_attributes pa ON pa.product = base.product
WHERE qe.product IS NOT NULL
    AND qe.warehouse IS NOT NULL
    AND qe.date IS NOT NULL
GROUP BY qe.warehouse, base.product, qe.date, pa.age, pa.product_category
ORDER BY qe.warehouse, base.product, qe.date

