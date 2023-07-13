WITH orders AS 
(
    SELECT 
        supplier_id,
        MAX(li.created_at) AS last_order_date,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) as days_since_last_publish,
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) <= 7 THEN 'active'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) > 7 AND DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) <= 30 THEN 'inactive'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) > 30 THEN 'churned'
            ELSE 'churned'
        END as Account_Status
    FROM  {{ ref('int_line_items') }} as li
    GROUP BY
        supplier_id

),

invoice_items AS
(
    SELECT
        supplier_id,
        COUNT(DISTINCT ii.invoice_header_id) as total_order_count_per_supplier,
        SUM(ii.price_without_tax) as total_order_value_per_supplier
    FROM  {{ ref('int_invoice_items') }} as ii
    GROUP BY
        supplier_id
)

SELECT


    s.*,

   -- o.last_order_date,


from   {{ ref('base_suppliers') }} as s 
LEFT JOIN orders as o ON s.supplier_id = o.supplier_id
LEFT JOIN invoice_items as ii ON s.supplier_id = ii.supplier_id
