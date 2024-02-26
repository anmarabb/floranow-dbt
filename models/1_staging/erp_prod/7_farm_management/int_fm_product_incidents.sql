
select

--product_incidents
    pi.fm_product_id,
    pi.incident_type,
    pi.stage,
    pi.quantity as incident_quantity,
    cast(pi.created_at as date) as incident_at,
    time_add(cast(pi.created_at as time), INTERVAL 3 hour) as Creation_time,


u.name as reported_by,

p.product_name,
p.astra_barcode,
p.fob_price,
p.sub_group,
p.color,



from   {{ ref('stg_fm_product_incidents') }} as pi
left join {{ ref('fct_fm_products') }} as p on pi.fm_product_id = p.fm_product_id
left join  {{ ref('base_users') }} as u on pi.reported_by_id = u.id

--where pi.incident_type <> 'DAMAGED'