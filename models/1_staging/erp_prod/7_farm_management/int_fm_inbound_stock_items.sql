
select

ins.*,

p.Product,
p.color,
p.sub_group,
p.stem_length,
p.bud_count,

p.fob_price,
p.astra_barcode,


sh.fm_shipment_id,

CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM production_date) = 1 AND EXTRACT(MONTH FROM production_date) = 12 THEN CAST(EXTRACT(YEAR FROM production_date) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM production_date) >= 52 AND EXTRACT(MONTH FROM production_date) = 1 THEN CAST(EXTRACT(YEAR FROM production_date) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM production_date) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM production_date) AS STRING)
) AS week_number,

from   {{ ref('stg_fm_inbound_stock_items') }} as ins
left join {{ ref('fct_fm_products') }} as p on p.fm_product_id = ins.fm_product_id


left join  {{ ref('stg_fm_box_items') }} as bi on ins.source_id = bi.fm_box_item_id and ins.source_type = 'Fm::BoxItem'
left join  {{ ref('stg_fm_boxes') }} as  box on box.fm_box_id = bi.fm_box_id
left join {{ ref('stg_fm_shipments') }}  as sh on sh.fm_shipment_id = box.fm_shipment_id