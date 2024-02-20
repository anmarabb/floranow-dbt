
select

outbound.*,
p.product_name,
p.color,
p.sub_group,
p.available_quantity,

from   {{ ref('stg_fm_outbound_stock_items') }} as outbound

left join {{ ref('fct_fm_products') }} as p on p.fm_product_id = outbound.fm_product_id