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
)

-- Select all columns from products filtered by top 100 products
SELECT 
  p.*
FROM {{ ref('fct_products') }} p
INNER JOIN top_100_products t100 ON t100.product = p.Product
ORDER BY p.warehouse, p.Product

