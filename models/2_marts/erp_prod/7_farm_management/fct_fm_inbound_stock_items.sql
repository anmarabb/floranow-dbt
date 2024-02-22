SELECT



--fm_products
    product_name,
    color,
    sub_group,
    inbound_quantity,
    week_number,
    production_date,
    stem_length,
    bud_count,

    fm_shipment_id,



FROM  {{ref('int_fm_inbound_stock_items')}} as db

