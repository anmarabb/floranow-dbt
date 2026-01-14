WITH date_range AS (
    SELECT date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        (SELECT MIN(date) FROM {{ ref('fct_daily_quantity_events') }}),
        (SELECT MAX(date) FROM {{ ref('fct_daily_quantity_events') }}),
        INTERVAL 1 DAY
    )) AS date
),

product_attributes AS (
    SELECT 
        product,
        MAX(taxon_age) AS age,
        ARRAY_AGG(product_category IGNORE NULLS ORDER BY CASE WHEN product_category IN ('Others', 'Other') THEN 1 ELSE 0 END, product_category LIMIT 1)[OFFSET(0)] AS product_category
    FROM {{ ref('stg_daily_demand_base') }}
    WHERE product IS NOT NULL
    GROUP BY product
),

product_warehouse_combos AS (
    SELECT DISTINCT
        product,
        warehouse
    FROM {{ ref('stg_daily_demand_base') }}
    WHERE product IS NOT NULL AND warehouse IS NOT NULL
),

all_combinations AS (
    SELECT 
        dr.date,
        pwc.warehouse,
        pwc.product,
        pa.age,
        pa.product_category
    FROM date_range dr
    CROSS JOIN product_warehouse_combos pwc
    LEFT JOIN product_attributes pa ON pa.product = pwc.product
)

SELECT 
    date,
    warehouse,
    product,
    age,
    product_category
FROM all_combinations
ORDER BY warehouse, product, date

