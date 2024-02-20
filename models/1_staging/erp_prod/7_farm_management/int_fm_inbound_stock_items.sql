
select

ins.*,

p.product_name,
p.color,
p.sub_group,
p.stem_length,
p.bud_count,


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