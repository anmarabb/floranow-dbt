SELECT



--fm_products
    product_name,
    color,
    sub_group,
    inbound_quantity,
    week_number,
    production_date,


FROM  {{ref('int_fm_inbound_stock_items')}} as db