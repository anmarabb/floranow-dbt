SELECT supplier_region as Origin,
       Supplier,
       product_name as Product,
       invoice_header_printed_at,
       delivery_date,
       --order_date,
       financial_administration,
       warehouse,
       line_item_id, 

FROM {{ref ("int_line_items")}}






