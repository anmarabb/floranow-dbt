
select

pli.*,

li.shipment_id,
li.raw_unit_fob_price,

from {{ ref('stg_package_line_items') }} as pli
left join {{ ref('stg_line_items') }} as li on li.line_item_id =pli.line_item_id