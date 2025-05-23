SELECT



--fm_products
    Product,
    color,
    sub_group,
    inbound_quantity,
    week_number,
    production_date,
    stem_length,
    bud_count,

    fm_shipment_id,
    fob_price,
    astra_barcode,

case when fm_shipment_id  in (3555,3506,3511) then 'Opening Production Stock' else 'Regular Production Stock' end as fm_report_filter,


FROM  {{ref('int_fm_inbound_stock_items')}} as db

