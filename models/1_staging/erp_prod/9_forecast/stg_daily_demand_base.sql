WITH 
-- Get top 100 products by total sold quantity
top_100_products AS (
    SELECT 
        p.product,
    FROM {{ ref('fct_products') }} p
    LEFT JOIN {{ ref('int_line_items') }} li ON li.line_item_id = p.line_item_id
    LEFT JOIN {{ ref('int_line_items') }} cli ON cli.parent_line_item_id = li.line_item_id
    WHERE cli.departure_date IS NOT NULL
        AND cli.ordered_quantity > 0
    GROUP BY p.product
    ORDER BY SUM(cli.ordered_quantity) DESC
    LIMIT 100
),

-- Get minimum date from data
min_date AS (
  SELECT 
    MIN(min_date) as start_date
  FROM (
    SELECT MIN(DATE(li.created_at)) as min_date FROM {{ ref('int_line_items') }} li WHERE li.created_at IS NOT NULL
    UNION ALL
    SELECT MIN(DATE(p.product_created_at)) as min_date FROM {{ ref('fct_products') }} p WHERE p.product_created_at IS NOT NULL
  )
),

-- Generate sequential date series
date_series AS (
  SELECT date
  FROM min_date md
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(md.start_date, CURRENT_DATE(), INTERVAL 1 DAY)) AS date
),

-- Get unique warehouse-product combinations for top 100 products
warehouse_products AS (
  SELECT DISTINCT
    p.warehouse,
    p.Product as product,
    -- t100.total_sold
  FROM {{ ref('fct_products') }} p
  INNER JOIN top_100_products t100 ON t100.product = p.Product
  WHERE p.warehouse IS NOT NULL 
    AND p.Product IS NOT NULL
)

-- Create base table with all combinations
SELECT 
  d.date,
  wp.warehouse,
  wp.product,
--   wp.total_sold as product_total_sold
FROM date_series d
CROSS JOIN warehouse_products wp
ORDER BY wp.warehouse, wp.product, d.date

