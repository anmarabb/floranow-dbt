select

st.*,

p.sub_group,
p.color,
p.available_quantity,
p.contract_status,
p.bud_count,
p.stem_length,
p.product_name,


from   {{ ref('stg_fm_stock_transactions') }} as st
left join {{ ref('fct_fm_products') }} as p on st.fm_product_id = p.fm_product_id
--left join  {{ ref('base_users') }} as u on pi.reported_by_id = u.id

